package modbus_plugin

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/goburrow/modbus"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	TYPE_REGISTER = 0
	TYPE_COIL     = 1
	TYPE_DISCRETE = 2
	TYPE_HOLDING  = 3
)

type modbusInput struct {
	endpoint         string
	subscription     []subscriptionDef
	slaveId          int
	subscribeEnabled bool
	client           modbus.Client
	timeout          time.Duration
	log              *service.Logger
	tcpHandler       *modbus.TCPClientHandler
}
type modbusTriggerInput struct {
	endpoint         string
	subscription     []subscriptionDef
	tSubscription    []tSubscriptionsDef
	slaveId          int
	subscribeEnabled bool
	client           modbus.Client
	timeout          time.Duration
	log              *service.Logger
	tcpHandler       *modbus.TCPClientHandler
}
type tSubscriptionsDef struct {
	ID   int
	tSub []tSubscription
}
type tSubscription struct {
	Name        string
	Address     uint16
	AddressType int
	Value       int16
}
type subscriptionDef struct {
	ID          int
	Name        string
	Group       string
	DB          string
	Historian   string
	SqlSp       string
	address     uint16
	addressType int
	value       int16
}

func ParseTSubscription(tSubscriptions []string) []tSubscriptionsDef {
	var parsedtSubscription []tSubscriptionsDef

	for _, jsonString := range tSubscriptions {

		var temp map[string][]map[string]string

		// Unmarshal the JSON string into the temporary map
		err := json.Unmarshal([]byte(jsonString), &temp)
		if err != nil {
			//log.Println(err)
			return parsedtSubscription
		}
		var tSubsc tSubscriptionsDef
		// Merge the temporary map into the result map
		for key, values := range temp {
			var tsub tSubscription
			var subNodes []tSubscription
			// create node - name mapping for tbatch nodes
			for _, obj := range values {

				tsub.Name = obj["name"]
				tsub.AddressType = getAddressType(obj["addresstype"])
				if err != nil {
					//log.Println(err)
					return parsedtSubscription
				}
				addr, err := strconv.Atoi(obj["address"])
				if err != nil {
					log.Println(err)
				}
				tsub.Address = uint16(addr)
				subNodes = append(subNodes, tsub)

			}
			tSubsc.ID, err = strconv.Atoi(key)
			tSubsc.tSub = subNodes
			log.Println("Key:", key, "tSubsc: ", tSubsc)

			parsedtSubscription = append(parsedtSubscription, tSubsc)
		}
	}
	return parsedtSubscription
}
func ParseSubscriptionDef(subscription []string) []subscriptionDef {
	var parsedsubscriptions []subscriptionDef
	for _, subscriptionElement := range subscription {

		var nodeMap map[string][]map[string]string
		var node subscriptionDef
		log.Println("node json:", subscriptionElement)
		err := json.Unmarshal([]byte(subscriptionElement), &nodeMap)
		if err != nil {
			log.Println("ParseNodeDef1", err)
			log.Println(err)
			return parsedsubscriptions
		}
		for key, values := range nodeMap {
			for _, obj := range values {

				node.ID, _ = strconv.Atoi(key)
				node.Group = obj["group"]
				node.DB = obj["db"]
				node.Historian = obj["historian"]
				node.SqlSp = obj["sqlSp"]
				node.Name = obj["name"]
				node.addressType = getAddressType(obj["addresstype"])
				addr, err := strconv.Atoi(obj["address"])
				if err != nil {
					log.Println(err)
				}
				node.address = uint16(addr)
			}
			//log.Println(node)
		}
		parsedsubscriptions = append(parsedsubscriptions, node)
	}

	return parsedsubscriptions

}
func getAddressType(addressType string) int {
	log.Println("address type: ", addressType)
	var addressT int
	switch addressType {
	case "inputregister":
		addressT = TYPE_REGISTER
	case "coils":
		addressT = TYPE_COIL
	case "discrete":
		addressT = TYPE_DISCRETE
	case "holding":
		addressT = TYPE_HOLDING
	default:
		addressT = TYPE_REGISTER
	}
	return addressT
}
func readModbusValue(client modbus.Client, address uint16, quantity uint16, addressType int) (int16, error) {
	if addressType == TYPE_REGISTER {
		results, err := client.ReadInputRegisters(address, quantity)
		if err != nil {
			return 0, err
		}
		data := binary.BigEndian.Uint16(results)

		return int16(data), nil

	} else if addressType == TYPE_COIL {
		results, err := client.ReadCoils(address, quantity)
		if err != nil {
			return 0, err
		}
		//data := binary.BigEndian.Uint16(results)
		if len(results) > 0 {
			value := int16(results[0])
			return value, nil
		}

	} else if addressType == TYPE_DISCRETE {
		results, err := client.ReadDiscreteInputs(address, quantity)
		if err != nil {
			return 0, err
		}
		//data := binary.BigEndian.Uint16(results)
		if len(results) > 0 {
			return int16(results[0]), nil
		}

	} else if addressType == TYPE_HOLDING {
		results, err := client.ReadHoldingRegisters(address, quantity)
		if err != nil {
			return 0, err
		}
		data := binary.BigEndian.Uint16(results)

		return int16(data), nil

	}
	return 0, nil

}
