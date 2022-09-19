/*
 * Copyright (c) 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Seth Hoenig
 *    Allan Stockdill-Mander
 *    Mike Robertson
 */

package mqtt

import (
	"bufio"
	"errors"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const closedNetConnErrorText = "use of closed network connection" // error string for closed conn (https://golang.org/src/net/error_test.go)

// ConnectMQTT takes a connected net.Conn and performs the initial MQTT handshake. Parameters are:
// conn - Connected net.Conn
// cm - Connect Packet with everything other than the protocol name/version populated (historical reasons)
// protocolVersion - The protocol version to attempt to connect with
//
// Note that, for backward compatibility, ConnectMQTT() suppresses the actual connection error (compare to connectMQTT()).
func ConnectMQTT(rw *bufio.ReadWriter, cm *packets.ConnectPacket, protocolVersion uint) (byte, bool) {
	rc, sessionPresent, _ := connectMQTT(rw, cm, protocolVersion)
	return rc, sessionPresent
}

func connectMQTT(rw *bufio.ReadWriter, cm *packets.ConnectPacket, protocolVersion uint) (byte, bool, error) {
	switch protocolVersion {
	case 3:
		DEBUG.Println(CLI, "Using MQTT 3.1 protocol")
		cm.ProtocolName = "MQIsdp"
		cm.ProtocolVersion = 3
	case 0x83:
		DEBUG.Println(CLI, "Using MQTT 3.1b protocol")
		cm.ProtocolName = "MQIsdp"
		cm.ProtocolVersion = 0x83
	case 0x84:
		DEBUG.Println(CLI, "Using MQTT 3.1.1b protocol")
		cm.ProtocolName = "MQTT"
		cm.ProtocolVersion = 0x84
	default:
		DEBUG.Println(CLI, "Using MQTT 3.1.1 protocol")
		cm.ProtocolName = "MQTT"
		cm.ProtocolVersion = 4
	}

	if err := cm.Write(rw.Writer); err != nil {
		ERROR.Println(CLI, err)
		return packets.ErrNetworkError, false, err
	}

	rc, sessionPresent, err := verifyCONNACK(rw.Reader)
	return rc, sessionPresent, err
}

// This function is only used for receiving a connack
// when the connection is first started.
// This prevents receiving incoming data while resume
// is in progress if clean session is false.
func verifyCONNACK(conn *bufio.Reader) (byte, bool, error) {
	DEBUG.Println(NET, "connect started")

	ca, err := packets.ReadPacket(conn)
	if err != nil {
		ERROR.Println(NET, "connect got error", err)
		return packets.ErrNetworkError, false, err
	}

	if ca == nil {
		ERROR.Println(NET, "received nil packet")
		return packets.ErrNetworkError, false, errors.New("nil CONNACK packet")
	}

	msg, ok := ca.(*packets.ConnackPacket)
	if !ok {
		ERROR.Println(NET, "received msg that was not CONNACK")
		return packets.ErrNetworkError, false, errors.New("non-CONNACK first packet received")
	}

	DEBUG.Println(NET, "received connack")
	return msg.ReturnCode, msg.SessionPresent, nil
}

// inbound encapsulates the output from startIncoming.
// err  - If != nil then an error has occurred
// cp - A control packet received over the network link
type inbound struct {
	err error
	cp  packets.ControlPacket
}

// startIncoming initiates a goroutine that reads incoming messages off the wire and sends them to the channel (returned).
// If there are any issues with the network connection then the returned channel will be closed and the goroutine will exit
// (so closing the connection will terminate the goroutine)
func startIncoming(reader *bufio.Reader) inbound {
	var err error
	var cp packets.ControlPacket

	DEBUG.Println(NET, "incoming started")

	if cp, err = packets.ReadPacket(reader); err != nil {
		// We do not want to log the error if it is due to the network connection having been closed
		// elsewhere (i.e. after sending DisconnectPacket). Detecting this situation is the subject of
		// https://github.com/golang/go/issues/4373
		if !strings.Contains(err.Error(), closedNetConnErrorText) {
			return inbound{err: err}
		}
		DEBUG.Println(NET, "incoming complete")
	}
	DEBUG.Println(NET, "startIncoming Received Message")
	return inbound{cp: cp}
}

// incomingComms encapsulates the possible output of the incomingComms routine. If err != nil then an error has occurred and
// the routine will have terminated; otherwise one of the other members should be non-nil
type incomingComms struct {
	err         error                  // If non-nil then there has been an error (ignore everything else)
	outbound    *PacketAndToken        // Packet (with token) than needs to be sent out (e.g. an acknowledgement)
	incomingPub *packets.PublishPacket // A new publish has been received; this will need to be passed on to our user
}

// startIncomingComms initiates incoming communications; this includes starting a goroutine to process incoming
// messages.
// Accepts a channel of inbound messages from the store (persisted messages); note this must be closed as soon as the
// everything in the store has been sent.
// Returns a channel that will be passed any received packets; this will be closed on a network error (and inboundFromStore closed)
func startIncomingComms(reader *bufio.Reader,
	c commsFns,
) *incomingComms {
	ibound := startIncoming(reader) // Start goroutine that reads from network connection

	DEBUG.Println(NET, "startIncomingComms started")

	var msg packets.ControlPacket
	// var ok bool

	if ibound.err != nil {
		ERROR.Println(ibound.err)
		return nil
	}

	if ibound.cp == nil {
		INFO.Println("conn read nil")
		// select {
		// case msg, ok = <-inboundFromStore:
		// 	if !ok {
		// 		return nil
		// 	}
		// default:
		// 	return nil
		// }
		return nil
	} else {
		DEBUG.Println(NET, "startIncomingComms: got msg on ibound")
		// If the inbound comms routine encounters any issues it will send us an error.
		if ibound.err != nil {
			return &incomingComms{err: ibound.err}
		}
		msg = ibound.cp
		c.persistInbound(msg)
		c.UpdateLastReceived() // Notify keepalive logic that we recently received a packet
	}

	switch m := msg.(type) {
	case *packets.PingrespPacket:
		DEBUG.Println(NET, "startIncomingComms: received pingresp")
		c.pingRespReceived()
	case *packets.SubackPacket:
		DEBUG.Println(NET, "startIncomingComms: received suback, id:", m.MessageID)
		token := c.getToken(m.MessageID)

		if t, ok := token.(*SubscribeToken); ok {
			DEBUG.Println(NET, "startIncomingComms: granted qoss", m.ReturnCodes)
			for i, qos := range m.ReturnCodes {
				t.subResult[t.subs[i]] = qos
			}
		}

		token.flowComplete()
		c.freeID(m.MessageID)
	case *packets.UnsubackPacket:
		DEBUG.Println(NET, "startIncomingComms: received unsuback, id:", m.MessageID)
		c.getToken(m.MessageID).flowComplete()
		c.freeID(m.MessageID)
	case *packets.PublishPacket:
		DEBUG.Println(NET, "startIncomingComms: received publish, msgId:", m.MessageID)
		return &incomingComms{incomingPub: m}
	case *packets.PubackPacket:
		DEBUG.Println(NET, "startIncomingComms: received puback, id:", m.MessageID)
		c.getToken(m.MessageID).flowComplete()
		c.freeID(m.MessageID)
	case *packets.PubrecPacket:
		DEBUG.Println(NET, "startIncomingComms: received pubrec, id:", m.MessageID)
		prel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
		prel.MessageID = m.MessageID
		return &incomingComms{outbound: &PacketAndToken{p: prel, t: nil}}
	case *packets.PubrelPacket:
		DEBUG.Println(NET, "startIncomingComms: received pubrel, id:", m.MessageID)
		pc := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pc.MessageID = m.MessageID
		c.persistOutbound(pc)
		return &incomingComms{outbound: &PacketAndToken{p: pc, t: nil}}
	case *packets.PubcompPacket:
		DEBUG.Println(NET, "startIncomingComms: received pubcomp, id:", m.MessageID)
		c.getToken(m.MessageID).flowComplete()
		c.freeID(m.MessageID)
	}

	return nil
}

func resolveIncomingComms(msg packets.ControlPacket,
	c commsFns,
) *incomingComms {

	c.persistInbound(msg)
	c.UpdateLastReceived()

	switch m := msg.(type) {
	case *packets.PingrespPacket:
		c.pingRespReceived()
	case *packets.SubackPacket:
		token := c.getToken(m.MessageID)
		if t, ok := token.(*SubscribeToken); ok {
			for i, qos := range m.ReturnCodes {
				t.subResult[t.subs[i]] = qos
			}
		}
		token.flowComplete()
		c.freeID(m.MessageID)
	case *packets.UnsubackPacket:
		c.getToken(m.MessageID).flowComplete()
		c.freeID(m.MessageID)
	case *packets.PublishPacket:
		return &incomingComms{incomingPub: m}
	case *packets.PubackPacket:
		c.getToken(m.MessageID).flowComplete()
		c.freeID(m.MessageID)
	case *packets.PubrecPacket:
		prel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
		prel.MessageID = m.MessageID
		return &incomingComms{outbound: &PacketAndToken{p: prel, t: nil}}
	case *packets.PubrelPacket:
		pc := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pc.MessageID = m.MessageID
		c.persistOutbound(pc)
		return &incomingComms{outbound: &PacketAndToken{p: pc, t: nil}}
	case *packets.PubcompPacket:
		c.getToken(m.MessageID).flowComplete()
		c.freeID(m.MessageID)
	}

	return nil
}

// startOutgoingComms initiates a go routine to transmit outgoing packets.
// Pass in an open network connection and channels for outbound messages (including those triggered
// directly from incoming comms).
// Returns a channel that will receive details of any errors (closed when the goroutine exits)
// This function wil only terminate when all input channels are closed
func startOutgoingComms(conn net.Conn,
	writer *bufio.Writer,
	c commsFns,
	oboundp <-chan *PacketAndToken,
	obound <-chan *PacketAndToken,
	oboundFromIncoming <-chan *PacketAndToken,
) error {

	DEBUG.Println(NET, "outgoing started")

	DEBUG.Println(NET, "outgoing waiting for an outbound message")

	// This goroutine will only exits when all of the input channels we receive on have been closed. This approach is taken to avoid any
	// deadlocks (if the connection goes down there are limited options as to what we can do with anything waiting on us and
	// throwing away the packets seems the best option)
	if oboundp == nil && obound == nil && oboundFromIncoming == nil {
		DEBUG.Println(NET, "outgoing comms stopping")
		return nil
	}

	select {
	case pub, ok := <-obound:
		if !ok {
			return nil
		}
		msg := pub.p.(*packets.PublishPacket)
		DEBUG.Println(NET, "obound msg to write", msg.MessageID)

		writeTimeout := c.getWriteTimeOut()
		if writeTimeout > 0 {
			if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				ERROR.Println(NET, "SetWriteDeadline ", err)
			}
		}

		if err := msg.Write(writer); err != nil {
			ERROR.Println(NET, "outgoing obound reporting error ", err)
			pub.t.setError(err)
			// report error if it's not due to the connection being closed elsewhere
			if !strings.Contains(err.Error(), closedNetConnErrorText) {
				return err
			}
			return nil
		}

		if writeTimeout > 0 {
			// If we successfully wrote, we don't want the timeout to happen during an idle period
			// so we reset it to infinite.
			if err := conn.SetWriteDeadline(time.Time{}); err != nil {
				ERROR.Println(NET, "SetWriteDeadline to 0 ", err)
			}
		}

		if msg.Qos == 0 {
			pub.t.flowComplete()
		}
		DEBUG.Println(NET, "obound wrote msg, id:", msg.MessageID)
	case msg, ok := <-oboundp:
		if !ok {
			return nil
		}
		DEBUG.Println(NET, "obound priority msg to write, type", reflect.TypeOf(msg.p))
		if err := msg.p.Write(writer); err != nil {
			ERROR.Println(NET, "outgoing oboundp reporting error ", err)
			if msg.t != nil {
				msg.t.setError(err)
			}
			return err
		}

		if _, ok := msg.p.(*packets.DisconnectPacket); ok {
			msg.t.(*DisconnectToken).flowComplete()
			DEBUG.Println(NET, "outbound wrote disconnect, closing connection")
			// As per the MQTT spec "After sending a DISCONNECT Packet the Client MUST close the Network Connection"
			// Closing the connection will cause the goroutines to end in sequence (starting with incoming comms)
			conn.Close()
		}
	case msg, ok := <-oboundFromIncoming: // message triggered by an inbound message (PubrecPacket or PubrelPacket)
		if !ok {
			return nil
		}
		DEBUG.Println(NET, "obound from incoming msg to write, type", reflect.TypeOf(msg.p), " ID ", msg.p.Details().MessageID)
		if err := msg.p.Write(writer); err != nil {
			ERROR.Println(NET, "outgoing oboundFromIncoming reporting error", err)
			if msg.t != nil {
				msg.t.setError(err)
			}
			return err
		}
	}
	c.UpdateLastSent() // Record that a packet has been received (for keepalive routine)
	return nil
}

// commsFns provide access to the client state (messageids, requesting disconnection and updating timing)
type commsFns interface {
	getToken(id uint16) tokenCompletor       // Retrieve the token for the specified messageid (if none then a dummy token must be returned)
	freeID(id uint16)                        // Release the specified messageid (clearing out of any persistent store)
	UpdateLastReceived()                     // Must be called whenever a packet is received
	UpdateLastSent()                         // Must be called whenever a packet is successfully sent
	getWriteTimeOut() time.Duration          // Return the writetimeout (or 0 if none)
	persistOutbound(m packets.ControlPacket) // add the packet to the outbound store
	persistInbound(m packets.ControlPacket)  // add the packet to the inbound store
	pingRespReceived()                       // Called when a ping response is received
}

// startComms initiates goroutines that handles communications over the network connection
// Messages will be stored (via commsFns) and deleted from the store as necessary
// It returns two channels:
//  packets.PublishPacket - Will receive publish packets received over the network.
//  Closed when incoming comms routines exit (on shutdown or if network link closed)
//  error - Any errors will be sent on this channel. The channel is closed when all comms routines have shut down
//
// Note: The comms routines monitoring oboundp and obound will not shutdown until those channels are both closed. Any messages received between the
// connection being closed and those channels being closed will generate errors (and nothing will be sent). That way the chance of a deadlock is
// minimised.
func startComms(conn net.Conn, // Network connection (must be active)
	c commsFns, // getters and setters to enable us to cleanly interact with client
	inboundFromStore <-chan packets.ControlPacket, // Inbound packets from the persistence store (should be closed relatively soon after startup)
	oboundp <-chan *PacketAndToken,
	obound <-chan *PacketAndToken,
) (
	<-chan *packets.PublishPacket, // Publishpackages received over the network
	<-chan error, // Any errors (should generally trigger a disconnect)
) {
	outPublish := make(chan *packets.PublishPacket)
	outError := make(chan error)
	return outPublish, outError
}

// ackFunc acknowledges a packet
// WARNING the function returned must not be called if the comms routine is shutting down or not running
// (it needs outgoing comms in order to send the acknowledgement). Currently this is only called from
// matchAndDispatch which will be shutdown before the comms are
func ackFunc(oboundP chan *ComnsPacket, persist Store, packet *packets.PublishPacket, client *client) func() {
	return func() {
		switch packet.Qos {
		case 2:
			pr := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
			pr.MessageID = packet.MessageID
			DEBUG.Println(NET, "putting pubrec msg on obound")
			oboundP <- &ComnsPacket{&PacketAndToken{p: pr, t: nil}, client}
			DEBUG.Println(NET, "done putting pubrec msg on obound")
		case 1:
			pa := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
			pa.MessageID = packet.MessageID
			DEBUG.Println(NET, "putting puback msg on obound")
			persistOutbound(persist, pa)
			oboundP <- &ComnsPacket{&PacketAndToken{p: pa, t: nil}, client}
			DEBUG.Println(NET, "done putting puback msg on obound")
		case 0:
			// do nothing, since there is no need to send an ack packet back
		}
	}
}
