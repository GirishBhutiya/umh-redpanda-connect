// Copyright 2023 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opcua_plugin

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	"github.com/gopcua/opcua/uatest"
)

type TNodeDef struct {
	TNodeID     *ua.NodeID
	NodeClass   ua.NodeClass
	BrowseName  string
	Description string
	AccessLevel ua.AccessLevelType
	Path        string
	DataType    string
	Writable    bool
	Unit        string
	Scale       string
	Min         string
	Max         string
}

func (n TNodeDef) Records() []string {
	return []string{n.BrowseName, n.DataType, n.TNodeID.String(), n.Unit, n.Scale, n.Min, n.Max, strconv.FormatBool(n.Writable), n.Description}
}

func tBrowse(ctx context.Context, n *opcua.Node, path string, level int, logger *service.Logger) ([]TNodeDef, error) {
	logger.Debugf("node:%s path:%q level:%d\n", n, path, level)
	if level > 10 {
		return nil, nil
	}

	attrs, err := n.Attributes(ctx, ua.AttributeIDNodeClass, ua.AttributeIDBrowseName, ua.AttributeIDDescription, ua.AttributeIDAccessLevel, ua.AttributeIDDataType)
	if err != nil {
		return nil, err
	}

	var def = TNodeDef{
		TNodeID: n.ID,
	}

	switch err := attrs[0].Status; err {
	case ua.StatusOK:
		def.NodeClass = ua.NodeClass(attrs[0].Value.Int())
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[1].Status; err {
	case ua.StatusOK:
		def.BrowseName = attrs[1].Value.String()
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[2].Status; err {
	case ua.StatusOK:
		def.Description = attrs[2].Value.String()
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[3].Status; err {
	case ua.StatusOK:
		def.AccessLevel = ua.AccessLevelType(attrs[3].Value.Int())
		def.Writable = def.AccessLevel&ua.AccessLevelTypeCurrentWrite == ua.AccessLevelTypeCurrentWrite
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	switch err := attrs[4].Status; err {
	case ua.StatusOK:
		switch v := attrs[4].Value.NodeID().IntID(); v {
		case id.DateTime:
			def.DataType = "time.Time"
		case id.Boolean:
			def.DataType = "bool"
		case id.SByte:
			def.DataType = "int8"
		case id.Int16:
			def.DataType = "int16"
		case id.Int32:
			def.DataType = "int32"
		case id.Byte:
			def.DataType = "byte"
		case id.UInt16:
			def.DataType = "uint16"
		case id.UInt32:
			def.DataType = "uint32"
		case id.UtcTime:
			def.DataType = "time.Time"
		case id.String:
			def.DataType = "string"
		case id.Float:
			def.DataType = "float32"
		case id.Double:
			def.DataType = "float64"
		default:
			def.DataType = attrs[4].Value.NodeID().String()
		}
	case ua.StatusBadAttributeIDInvalid:
		// ignore
	case ua.StatusBadSecurityModeInsufficient:
		return nil, nil
	default:
		return nil, err
	}

	def.Path = join(path, def.BrowseName)
	logger.Debugf("%d: def.Path:%s def.NodeClass:%s\n", level, def.Path, def.NodeClass)

	var nodes []TNodeDef
	// If a node has a Variable class, it probably means that it is a tag
	// Therefore, no need to browse further
	if def.NodeClass == ua.NodeClassVariable {
		nodes = append(nodes, def)
		return nodes, nil
	}

	browseChildren := func(refType uint32) error {
		refs, err := n.ReferencedNodes(ctx, refType, ua.BrowseDirectionForward, ua.NodeClassAll, true)
		if err != nil {
			return errors.Errorf("References: %d: %s", refType, err)
		}
		logger.Debugf("found %d child refs\n", len(refs))
		for _, rn := range refs {
			children, err := tBrowse(ctx, rn, def.Path, level+1, logger)
			if err != nil {
				return errors.Errorf("browse children: %s", err)
			}
			nodes = append(nodes, children...)
		}
		return nil
	}

	// If a node has an Object class, it probably means that it is a folder
	// Therefore, browse its children
	if def.NodeClass == ua.NodeClassObject {
		// To determine if an Object is a folder, we need to check different references
		// Add here all references that should be checked
		if err := browseChildren(id.HasComponent); err != nil {
			return nil, err
		}
		if err := browseChildren(id.Organizes); err != nil {
			return nil, err
		}
		if err := browseChildren(id.FolderType); err != nil {
			return nil, err
		}
		// For hasProperty it makes sense to show it very close to the tag itself, e.g., use the tagName as tagGroup and then the properties as subparts of it
		/*
			if err := browseChildren(id.HasProperty); err != nil {
				return nil, err
			}
		*/
	}
	return nodes, nil
}

//------------------------------------------------------------------------------

var OPCUATriConfigSpec = service.NewConfigSpec().
	Summary("Creates an input that reads data from OPC-UA servers").
	Field(service.NewStringField("endpoint").Description("Address of the OPC-UA server to connect with.")).
	Field(service.NewStringField("username").Description("Username for server access. If not set, no username is used.").Default("")).
	Field(service.NewStringField("password").Description("Password for server access. If not set, no password is used.").Default("")).
	//Field(service.NewStringListField("nodeIDs").Description("List of OPC-UA node IDs to begin browsing.")).
	Field(service.NewStringListField("tNodeIDs").Description("List of OPC-UA trigger node IDs.")).
	Field(service.NewStringListField("tBatchNodeIDs").Description("List of OPC-UA trigger batch node IDs.")).
	Field(service.NewStringField("securityMode").Description("Security mode to use. If not set, a reasonable security mode will be set depending on the discovered endpoints.").Default("")).
	Field(service.NewStringField("securityPolicy").Description("The security policy to use.  If not set, a reasonable security policy will be set depending on the discovered endpoints.").Default("")).
	Field(service.NewBoolField("insecure").Description("Set to true to bypass secure connections, useful in case of SSL or certificate issues. Default is secure (false).").Default(false)).
	Field(service.NewBoolField("subscribeEnabled").Description("Set to true to subscribe to OPC-UA nodes instead of fetching them every seconds. Default is pulling messages every second (false).").Default(false))

func newOPCUATriInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	endpoint, err := conf.FieldString("endpoint")
	if err != nil {
		return nil, err
	}

	securityMode, err := conf.FieldString("securityMode")
	if err != nil {
		return nil, err
	}

	securityPolicy, err := conf.FieldString("securityPolicy")
	if err != nil {
		return nil, err
	}

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, err
	}

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, err
	}

	insecure, err := conf.FieldBool("insecure")
	if err != nil {
		return nil, err
	}

	subscribeEnabled, err := conf.FieldBool("subscribeEnabled")
	if err != nil {
		return nil, err
	}

	/* nodeIDs, err := conf.FieldStringList("nodeIDs")
	if err != nil {
		return nil, err
	} */

	tNodeIDs, err := conf.FieldStringList("tNodeIDs")
	if err != nil {
		return nil, err
	}

	tBatchNodeIDs, err := conf.FieldStringList("tBatchNodeIDs")
	if err != nil {
		return nil, err
	}

	// fail if no nodeIDs are provided
	if len(tNodeIDs) == 0 {
		return nil, errors.New("no tNodeIDs provided")
	}

	parsedNodeIDs := ParseNodeIDs(tNodeIDs)
	parsedTNodeIDs := ParseTriggerNodeIDs(tNodeIDs)

	// tnodes group, sql procedures mapping
	tNodeGroupMapping := make(map[string]string)
	tNodesMapping := make(map[string]string)

	// for _, node := range tNodeIDs {
	// 	var temp map[string][]string
	// 	err := json.Unmarshal([]byte(node), &temp)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	for _, value := range temp {
	// 		tNodeID := value[0]
	// 		groupAndSql := fmt.Sprintf("%s,%s", value[1], value[2])
	// 		tNodeGroupMapping[tNodeID] = groupAndSql
	// 	}
	// }

	for _, tNodeElements := range tNodeIDs {

		var tNodeObj map[string][]map[string]string
		err := json.Unmarshal([]byte(tNodeElements), &tNodeObj)
		if err != nil {
			return nil, err
		}

		for key, values := range tNodeObj {
			for _, obj := range values {
				tNodeID := obj["node"]
				group := obj["group"]
				db := obj["db"]
				historian := obj["historian"]
				sqlSp := obj["sqlSp"]

				tNodeElement := fmt.Sprintf("%s,%s,%s,%s", group, db, historian, sqlSp)
				tNodeGroupMapping[tNodeID] = tNodeElement
				tNodesMapping[tNodeID] = key
			}
		}
	}

	// for _, node := range tNodeIDs {
	// 	var temp map[string][]string
	// 	err := json.Unmarshal([]byte(node), &temp)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	for key, val := range temp {
	// 		tNodesMapping[val[0]] = key
	// 	}
	// }

	// parse tbatch node ids
	tBatchNodesMapping := make(map[string][]*ua.NodeID)
	tBatchNodeNameMapping := make(map[string]string)
	tBatch := make([]string, 0)

	for _, jsonString := range tBatchNodeIDs {

		var temp map[string][]map[string]string

		// Unmarshal the JSON string into the temporary map
		err := json.Unmarshal([]byte(jsonString), &temp)
		if err != nil {
			return nil, err
		}

		// Merge the temporary map into the result map
		for key, values := range temp {

			var tempNodeIDs []string
			// create node - name mapping for tbatch nodes
			for _, obj := range values {
				tBatchNodeNameMapping[obj["node"]] = obj["name"]
				tempNodeIDs = append(tempNodeIDs, obj["node"])
			}
			parsedNodeIDs := ParseNodeIDs(tempNodeIDs)
			tBatchNodesMapping[key] = append(tBatchNodesMapping[key], parsedNodeIDs...)
			tBatch = append(tBatch, tempNodeIDs...)
		}
	}
	parsedBatchTNodeIDs := ParseNodeIDs(tBatch)

	m := &OPCUATriInput{
		endpoint:              endpoint,
		username:              username,
		password:              password,
		nodeIDs:               parsedNodeIDs,
		tNodeIDs:              parsedTNodeIDs,
		batchTNodeIDs:         parsedBatchTNodeIDs,
		tBatchNodesMapping:    tBatchNodesMapping,
		tNodesMapping:         tNodesMapping,
		tBatchNodeNameMapping: tBatchNodeNameMapping,
		tNodeGroupMapping:     tNodeGroupMapping,
		log:                   mgr.Logger(),
		securityMode:          securityMode,
		securityPolicy:        securityPolicy,
		insecure:              insecure,
		subscribeEnabled:      subscribeEnabled,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {

	err := service.RegisterBatchInput(
		"opcuatrigger", OPCUATriConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newOPCUATriInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type OPCUATriInput struct {
	endpoint              string
	username              string
	password              string
	nodeIDs               []*ua.NodeID
	tNodeIDs              []*ua.NodeID
	batchTNodeIDs         []*ua.NodeID
	tNodesMapping         map[string]string
	tBatchNodesMapping    map[string][]*ua.NodeID
	nodeList              []TNodeDef
	tNodeList             []TNodeDef
	batchTNodeList        map[string][]TNodeDef
	tBatchNodeNameMapping map[string]string
	tNodeGroupMapping     map[string]string
	securityMode          string
	securityPolicy        string
	insecure              bool
	client                *opcua.Client
	log                   *service.Logger
	// this is required for subscription
	subscribeEnabled bool
	subNotifyChan    chan *opcua.PublishNotificationData
}

func (g *OPCUATriInput) Connect(ctx context.Context) error {

	if g.client != nil {
		return nil
	}

	var c *opcua.Client
	var endpoints []*ua.EndpointDescription
	var err error

	// Step 1: Retrieve all available endpoints from the OPC UA server.
	g.log.Infof("Endpoint URI: %s", g.endpoint)
	endpoints, err = opcua.GetEndpoints(ctx, g.endpoint)
	if err != nil {
		g.log.Infof("GetEndpoints failed: %s", err)
	}

	// Step 2: Log details of each discovered endpoint for debugging.
	for i, endpoint := range endpoints {
		g.log.Infof("Endpoint %d:", i+1)
		g.log.Infof("  EndpointURL: %s", endpoint.EndpointURL)
		g.log.Infof("  SecurityMode: %v", endpoint.SecurityMode)
		g.log.Infof("  SecurityPolicyURI: %s", endpoint.SecurityPolicyURI)
		g.log.Infof("  TransportProfileURI: %s", endpoint.TransportProfileURI)
		g.log.Infof("  SecurityLevel: %d", endpoint.SecurityLevel)

		// If Server is not nil, log its details
		if endpoint.Server != nil {
			g.log.Infof("  Server ApplicationURI: %s", endpoint.Server.ApplicationURI)
			g.log.Infof("  Server ProductURI: %s", endpoint.Server.ProductURI)
			g.log.Infof("  Server ApplicationName: %s", endpoint.Server.ApplicationName.Text)
			g.log.Infof("  Server ApplicationType: %v", endpoint.Server.ApplicationType)
			g.log.Infof("  Server GatewayServerURI: %s", endpoint.Server.GatewayServerURI)
			g.log.Infof("  Server DiscoveryProfileURI: %s", endpoint.Server.DiscoveryProfileURI)
			g.log.Infof("  Server DiscoveryURLs: %v", endpoint.Server.DiscoveryURLs)
		}

		// Output the certificate
		if len(endpoint.ServerCertificate) > 0 {
			// Convert to PEM format first, then log the certificate information
			pemCert := pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: endpoint.ServerCertificate,
			})
			g.logCertificateInfo(pemCert)
		}

		// Loop through UserIdentityTokens
		for j, token := range endpoint.UserIdentityTokens {
			g.log.Infof("  UserIdentityToken %d:", j+1)
			g.log.Infof("    PolicyID: %s", token.PolicyID)
			g.log.Infof("    TokenType: %v", token.TokenType)
			g.log.Infof("    IssuedTokenType: %s", token.IssuedTokenType)
			g.log.Infof("    IssuerEndpointURL: %s", token.IssuerEndpointURL)
		}
	}

	// Step 3: Determine the authentication method to use.
	// Default to Anonymous if neither username nor password is provided.
	selectedAuthentication := ua.UserTokenTypeAnonymous
	if g.username != "" && g.password != "" {
		// Use UsernamePassword authentication if both username and password are available.
		selectedAuthentication = ua.UserTokenTypeUserName
	}

	// Step 3.1: Filter the endpoints based on the selected authentication method.
	// This will eliminate endpoints that do not support the chosen method.
	selectedEndpoint := g.getReasonableEndpoint(endpoints, selectedAuthentication, g.insecure, g.securityMode, g.securityPolicy)
	if selectedEndpoint == nil {
		g.log.Errorf("Could not select a suitable endpoint")
		return err
	}
	if strings.HasPrefix(selectedEndpoint.EndpointURL, "opc.tcp://:") { // I omitted the port here, as it might change ?
		selectedEndpoint.EndpointURL = g.endpoint
	}
	g.log.Infof("Selected endpoint: %v", selectedEndpoint)

	// Step 4: Initialize OPC UA client options
	opts := make([]opcua.Option, 0)
	opts = append(opts, opcua.SecurityFromEndpoint(selectedEndpoint, selectedAuthentication))

	// Set additional options based on the authentication method
	switch selectedAuthentication {
	case ua.UserTokenTypeAnonymous:
		g.log.Infof("Using anonymous login")
	case ua.UserTokenTypeUserName:
		g.log.Infof("Using username/password login")
		opts = append(opts, opcua.AuthUsername(g.username, g.password))
	}

	// Step 5: Generate Certificates, because this is really a step that can not happen in the background...
	if !g.insecure {
		// Generate a new certificate in memory, no file read/write operations.
		randomStr := randomString(8) // Generates an 8-character random string
		clientName := "urn:benthos-umh:client-" + randomStr
		certPEM, keyPEM, err := uatest.GenerateCert(clientName, 2048, 24*time.Hour*365*10)
		if err != nil {
			g.log.Errorf("Failed to generate certificate: %v", err)
			return err
		}

		// Convert PEM to X509 Certificate and RSA PrivateKey for in-memory use.
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			g.log.Errorf("Failed to parse certificate: %v", err)
			return err
		}

		pk, ok := cert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			g.log.Errorf("Invalid private key type")
			return err
		}

		// Append the certificate and private key to the client options
		opts = append(opts, opcua.PrivateKey(pk), opcua.Certificate(cert.Certificate[0]))
	}

	// Step 6: Create and connect the OPC UA client
	// Note that we are not taking `selectedEndpoint.EndpointURL` here as the server can be misconfigured. We are taking instead the user input.
	c, err = opcua.NewClient(g.endpoint, opts...)
	if err != nil {
		g.log.Errorf("Failed to create a new client")
		return err
	}

	// Connect to the selected endpoint
	if err := c.Connect(ctx); err != nil {
		g.log.Errorf("Failed to connect")
		return err
	}

	g.log.Infof("Connected to %s", g.endpoint)
	g.log.Infof("Please note that browsing large node trees can take a long time (around 5 nodes per second)")

	g.client = c

	// Create a slice to store the detected nodes
	nodeList := make([]TNodeDef, 0)

	// Print all nodeIDs that are being browsed
	for _, id := range g.nodeIDs {
		if id == nil {
			continue
		}

		// Print id
		g.log.Infof("Browsing nodeID: %s", id.String())

		// Browse the OPC-UA server's node tree and print the results.
		nodes, err := tBrowse(ctx, g.client.Node(id), "", 0, g.log)
		if err != nil {
			g.log.Errorf("Browsing failed: %s")
			c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		// Add the nodes to the nodeList
		nodeList = append(nodeList, nodes...)
	}

	b, err := json.Marshal(nodeList)
	if err != nil {
		g.log.Errorf("Unmarshalling failed: %s")
		c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
		return err
	}

	g.log.Infof("Detected nodes: %s", b)

	g.nodeList = nodeList

	// to detect all trigger nodes
	if err := g.detectTriggerNodeIDs(ctx); err != nil {
		g.log.Errorf("Error detecting trigger nodes: %s")
		c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
		return err
	}
	// to detect all trigger nodes
	if err := g.detectBatchTriggerNodeIDs(ctx); err != nil {
		g.log.Errorf("Error detecting batch trigger nodes: %s")
		c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
		return err
	}

	// If subscription is enabled, start subscribing to the nodes
	if g.subscribeEnabled {
		g.log.Infof("Subscription is enabled, therefore start subscribing to the selected nodes...")

		g.subNotifyChan = make(chan *opcua.PublishNotificationData, 100)

		sub, err := c.Subscribe(ctx, &opcua.SubscriptionParameters{
			Interval: opcua.DefaultSubscriptionInterval,
		}, g.subNotifyChan)
		if err != nil {
			g.log.Errorf("Subscribing failed: %s")
			c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}

		monitoredRequests := make([]*ua.MonitoredItemCreateRequest, 0, len(g.tNodeList))

		for pos, id := range g.tNodeList {
			miCreateRequest := opcua.NewMonitoredItemCreateRequestWithDefaults(id.TNodeID, ua.AttributeIDValue, uint32(pos))
			monitoredRequests = append(monitoredRequests, miCreateRequest)
		}

		if len(g.tNodeList) == 0 {
			g.log.Errorf("Did not subscribe to any nodes. This can happen if the nodes that are selected are incompatible with this benthos version. Aborting...")
			return fmt.Errorf("no valid nodes selected")
		}

		res, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, monitoredRequests...)
		if err != nil {
			g.log.Errorf("Monitoring failed: %s")
			c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return err
		}
		if res == nil {
			g.log.Errorf("Expected res to not be nil, if there is no error")
			c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
			return fmt.Errorf("expected res to be not nil")
		}

		// Assuming you want to check the status code of each result
		for _, result := range res.Results {
			if !errors.Is(result.StatusCode, ua.StatusOK) {
				g.log.Errorf("Monitoring failed with status code: %v", result.StatusCode)
				c.Close(ctx) // ensure that if something fails here, the connection is always safely closed
				return fmt.Errorf("monitoring failed for node, status code: %v", result.StatusCode)
			}
		}

		g.log.Infof("Subscribed to %d nodes!", len(res.Results))

	}

	return nil
}

// createMessageFromValue creates a benthos messages from a given variant and nodeID
// theoretically nodeID can be extracted from variant, but not in all cases (e.g., when subscribing), so it it left to the calling function
func (g *OPCUATriInput) createMessageFromValue(variant *ua.Variant, node TNodeDef, nodeID string, msgs map[string]string) *service.Message {

	if variant == nil {
		g.log.Errorf("Variant is nil")
		return nil
	}

	b := make([]byte, 0)

	switch v := variant.Value().(type) {
	case float32:
		b = append(b, []byte(strconv.FormatFloat(float64(v), 'f', -1, 32))...)
	case float64:
		b = append(b, []byte(strconv.FormatFloat(v, 'f', -1, 64))...)
	case string:
		b = append(b, []byte(string(v))...)
	case bool:
		b = append(b, []byte(strconv.FormatBool(v))...)
	case int:
		b = append(b, []byte(strconv.Itoa(v))...)
	case int8:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int16:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int32:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int64:
		b = append(b, []byte(strconv.FormatInt(v, 10))...)
	case uint:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint8:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint16:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint32:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint64:
		b = append(b, []byte(strconv.FormatUint(v, 10))...)
	default:
		// Convert unknown types to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			g.log.Errorf("Error marshaling to JSON: %v", err)
			return nil
		}
		b = append(b, jsonBytes...)
	}

	if b == nil {
		g.log.Errorf("Could not create benthos message as payload is empty for node %s: %v", nodeID, b)
		return nil
	}

	message := service.NewMessage(b)

	re := regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	opcuaPath := re.ReplaceAllString(nodeID, "_")
	message.MetaSet("opcua_path", opcuaPath)
	message.MetaSet("description", node.Description)
	message.MetaSet("nodeID", nodeID)

	tNodeElements := strings.Split(g.tNodeGroupMapping[nodeID], ",")

	message.MetaSet("group", tNodeElements[0])
	message.MetaSet("db", tNodeElements[1])
	message.MetaSet("historian", tNodeElements[2])
	message.MetaSet("sqlSp", tNodeElements[3])

	newBatchNodes := make(map[string]string)

	for nodeID, val := range msgs {
		newdNode := re.ReplaceAllString(nodeID, "_")
		newBatchNodes[newdNode] = val
	}

	jsonMsg, err := json.Marshal(newBatchNodes)
	if err != nil {
		g.log.Errorf("Could not change benthos message to json object")
		return nil
	}
	message.MetaSet("Message", string(jsonMsg))
	return message
}

func (g *OPCUATriInput) fetchMessages(variant *ua.Variant, node TNodeDef, nodeID string) (string, string) {
	if variant == nil {
		g.log.Errorf("Variant is nil")
		return "", ""
	}

	b := make([]byte, 0)

	switch v := variant.Value().(type) {
	case float32:
		b = append(b, []byte(strconv.FormatFloat(float64(v), 'f', -1, 32))...)
	case float64:
		b = append(b, []byte(strconv.FormatFloat(v, 'f', -1, 64))...)
	case string:
		b = append(b, []byte(string(v))...)
	case bool:
		b = append(b, []byte(strconv.FormatBool(v))...)
	case int:
		b = append(b, []byte(strconv.Itoa(v))...)
	case int8:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int16:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int32:
		b = append(b, []byte(strconv.FormatInt(int64(v), 10))...)
	case int64:
		b = append(b, []byte(strconv.FormatInt(v, 10))...)
	case uint:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint8:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint16:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint32:
		b = append(b, []byte(strconv.FormatUint(uint64(v), 10))...)
	case uint64:
		b = append(b, []byte(strconv.FormatUint(v, 10))...)
	default:
		// Convert unknown types to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			g.log.Errorf("Error marshaling to JSON: %v", err)
			return "", ""
		}
		b = append(b, jsonBytes...)
	}

	if b == nil {
		g.log.Errorf("Could not create benthos message as payload is empty for node %s: %v", nodeID, b)
		return "", ""
	}

	// return fmt.Sprintf("%s: %s", nodeID, string(b))
	return g.tBatchNodeNameMapping[nodeID], string(b)
}

func (g *OPCUATriInput) ReadBatchPull(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.client == nil {
		return nil, nil, errors.New("client is nil")
	}
	// Read all values in NodeList and return each of them as a message with the node's path as the metadata

	// Create first a list of all the values to read
	var nodesToRead []*ua.ReadValueID

	for _, node := range g.nodeList {
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID: node.TNodeID,
		})
	}

	if len(g.nodeList) > 100 {
		g.log.Warnf("Reading more than 100 nodes with pull method. The request might fail as it can take too much time. Recommendation: use subscribeEnabled: true instead for better performance")
	}

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := g.client.Read(ctx, req)
	if err != nil {
		g.log.Errorf("Read failed: %s", err)
		// if the error is StatusBadSessionIDInvalid, the session has been closed
		// and we need to reconnect.
		switch err {
		case ua.StatusBadSessionIDInvalid:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadCommunicationError:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadConnectionClosed:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadTimeout:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadConnectionRejected:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadServerNotConnected:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		}

		// return error and stop executing this function.
		return nil, nil, err
	}

	if resp.Results[0].Status != ua.StatusOK {
		g.log.Errorf("Status not OK: %v", resp.Results[0].Status)
	}

	// Create a message with the node's path as the metadata
	msgs := service.MessageBatch{}

	for i, node := range g.nodeList {
		value := resp.Results[i].Value
		if value == nil {
			g.log.Errorf("Received nil from node: %s", node.TNodeID.String())
			continue
		}
		message := g.createMessageFromValue(value, node, node.TNodeID.String(), nil)
		if message != nil {
			msgs = append(msgs, message)
		}
	}

	// Wait for a second before returning a message.
	time.Sleep(time.Second)

	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *OPCUATriInput) ReadBatchSubscribe(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	var res *opcua.PublishNotificationData

	if ctx == nil || ctx.Done() == nil {
		return nil, nil, errors.New("emptyCtx is invalid for ReadBatchSubscribe")
	}
	select {
	case res = <-g.subNotifyChan:
		// Received a result, check for error
		if res.Error != nil {
			g.log.Errorf("ReadBatchSubscribe error: %s", res.Error)
			return nil, nil, res.Error
		}

		if g.nodeList == nil {
			g.log.Errorf("nodelist is nil")
			return nil, nil, errors.New("nodelist empty")
		}

		// Create a message with the node's path as the metadata
		msgs := service.MessageBatch{}

		switch x := res.Value.(type) {
		case *ua.DataChangeNotification:
			for _, item := range x.MonitoredItems {
				if item == nil || item.Value == nil || item.Value.Value == nil {
					g.log.Errorf("Received nil in item structure")
					continue
				}

				// now get the handle id, which is the position in g.Nodelist
				// see also NewMonitoredItemCreateRequestWithDefaults call in other functions
				handleID := item.ClientHandle

				if uint32(len(g.nodeList)) >= handleID {
					message := g.createMessageFromValue(item.Value.Value, g.nodeList[handleID], g.nodeList[handleID].TNodeID.String(), nil)
					if message != nil {
						msgs = append(msgs, message)
					}
				}
			}
		default:
			g.log.Errorf("Unknown publish result %T", res.Value)
		}

		return msgs, func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			return nil
		}, nil

	case _, ok := <-ctx.Done():
		if !ok {
			g.log.Errorf("timeout channel was closed")
		} else {
			// Timeout occurred
			g.log.Error("Timeout waiting for response from g.subNotifyChan")
		}
		return nil, nil, errors.New("timeout waiting for response")
	}
}

func (g *OPCUATriInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if g.subscribeEnabled {
		return g.ReadTriggerBatchSubscribe(ctx)
		// return g.ReadBatchSubscribe(ctx)
	}
	// return g.ReadBatchPull(ctx)
	// return g.ReadTriggerBatchPull(ctx)
	return nil, nil, nil
}

func (g *OPCUATriInput) Close(ctx context.Context) error {
	if g.client != nil {
		g.client.Close(ctx)
		g.client = nil
	}

	return nil
}

func (g *OPCUATriInput) logCertificateInfo(certBytes []byte) {
	g.log.Infof("  Server certificate:")

	// Decode the certificate from base64 to DER format
	block, _ := pem.Decode(certBytes)
	if block == nil {
		g.log.Errorf("Failed to decode certificate")
		return
	}

	// Parse the DER-format certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		g.log.Errorf("Failed to parse certificate:", err)
		return
	}

	// Log the details
	g.log.Infof("    Not Before:", cert.NotBefore)
	g.log.Infof("    Not After:", cert.NotAfter)
	g.log.Infof("    DNS Names:", cert.DNSNames)
	g.log.Infof("    IP Addresses:", cert.IPAddresses)
	g.log.Infof("    URIs:", cert.URIs)
}

func (g *OPCUATriInput) detectTriggerNodeIDs(ctx context.Context) error {

	// Create a slice to store the detected trigger nodes
	tNodeList := make([]TNodeDef, 0)

	// Print all trigger nodeIDs that are being browsed
	for _, id := range g.tNodeIDs {
		if id == nil {
			continue
		}

		// Print id
		g.log.Infof("Browsing trigger nodeID: %s", id.String())

		// Browse the OPC-UA server's node tree and print the results.
		nodes, err := tBrowse(ctx, g.client.Node(id), "", 0, g.log)
		if err != nil {
			g.log.Errorf("Browsing failed: %s")
			return err
		}

		// Add the trigger nodes to the tNodeList
		tNodeList = append(tNodeList, nodes...)
	}

	b, err := json.Marshal(tNodeList)
	if err != nil {
		g.log.Errorf("Unmarshalling failed: %s")
		return err
	}

	g.log.Infof("Detected Trigger nodes: %s", b)

	g.tNodeList = tNodeList
	return nil
}

func (g *OPCUATriInput) detectBatchTriggerNodeIDs(ctx context.Context) error {

	// Create a slice to store the detected trigger nodes
	batchTNodeListMapping := make(map[string][]TNodeDef, 0)

	// Print all trigger nodeIDs that are being browsed
	for key, nodes := range g.tBatchNodesMapping {

		batchTNodeList := make([]TNodeDef, 0)
		for _, id := range nodes {
			if id == nil {
				continue
			}

			// Print id
			g.log.Infof("Browsing batch trigger nodeID: %s", id.String())

			// Browse the OPC-UA server's node tree and print the results.
			nodes, err := tBrowse(ctx, g.client.Node(id), "", 0, g.log)
			if err != nil {
				g.log.Errorf("Browsing failed: %s")
				return err
			}

			// Add the trigger nodes to the tNodeList
			batchTNodeList = append(batchTNodeList, nodes...)
		}
		batchTNodeListMapping[key] = batchTNodeList
	}

	b, err := json.Marshal(batchTNodeListMapping)
	if err != nil {
		g.log.Errorf("Unmarshalling failed: %s")
		return err
	}

	g.log.Infof("Detected batch Trigger nodes: %s", b)

	g.batchTNodeList = batchTNodeListMapping
	return nil
}

func (g *OPCUATriInput) ReadTriggerBatchPull(ctx context.Context, node TNodeDef, nodeID string) (map[string]string, service.AckFunc, error) {
	if g.client == nil {
		return nil, nil, errors.New("client is nil")
	}

	tBatchNodesKey := g.tNodesMapping[nodeID]
	tBatchNodeIDs := g.batchTNodeList[tBatchNodesKey]

	// g.log.Infof("Trigger Batch Nodes: ", tBatchNodeIDs)
	// Read all values in NodeList and return each of them as a message with the node's path as the metadata

	// Create first a list of all the values to read
	var nodesToRead []*ua.ReadValueID

	for _, node := range tBatchNodeIDs {
		nodesToRead = append(nodesToRead, &ua.ReadValueID{
			NodeID: node.TNodeID,
		})
	}

	if len(tBatchNodeIDs) > 100 {
		g.log.Warnf("Reading more than 100 nodes with pull method. The request might fail as it can take too much time. Recommendation: use subscribeEnabled: true instead for better performance")
	}

	req := &ua.ReadRequest{
		MaxAge:             2000,
		NodesToRead:        nodesToRead,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := g.client.Read(ctx, req)
	if err != nil {
		g.log.Errorf("Read failed: %s", err)
		// if the error is StatusBadSessionIDInvalid, the session has been closed
		// and we need to reconnect.
		switch err {
		case ua.StatusBadSessionIDInvalid:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadCommunicationError:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadConnectionClosed:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadTimeout:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadConnectionRejected:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		case ua.StatusBadServerNotConnected:
			g.client.Close(ctx)
			g.client = nil
			return nil, nil, service.ErrNotConnected
		}

		// return error and stop executing this function.
		return nil, nil, err
	}

	if resp.Results[0].Status != ua.StatusOK {
		g.log.Errorf("Status not OK: %v", resp.Results[0].Status)
	}

	// Create a message with the node's path as the metadata
	msgs := make(map[string]string, 0)

	for i, node := range tBatchNodeIDs {
		value := resp.Results[i].Value
		if value == nil {
			g.log.Errorf("Received nil from batch trigger node: %s", node.TNodeID.String())
			continue
		}
		nodeID, change := g.fetchMessages(value, node, node.TNodeID.String())
		if nodeID != "" {
			msgs[nodeID] = change
		}
	}
	return msgs, func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (g *OPCUATriInput) ReadTriggerBatchSubscribe(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	var res *opcua.PublishNotificationData

	if ctx == nil || ctx.Done() == nil {
		return nil, nil, errors.New("emptyCtx is invalid for ReadBatchSubscribe")
	}
	select {
	case res = <-g.subNotifyChan:
		// Received a result, check for error
		if res.Error != nil {
			g.log.Errorf("ReadBatchSubscribe error: %s", res.Error)
			return nil, nil, res.Error
		}

		if g.tNodeList == nil {
			g.log.Errorf("nodelist is nil")
			return nil, nil, errors.New("nodelist empty")
		}

		// Create a message with the node's path as the metadata
		msgs := service.MessageBatch{}

		switch x := res.Value.(type) {
		case *ua.DataChangeNotification:
			for _, item := range x.MonitoredItems {
				if item == nil || item.Value == nil || item.Value.Value == nil {
					g.log.Errorf("Received nil in item structure")
					continue
				}

				// now get the handle id, which is the position in g.Nodelist
				// see also NewMonitoredItemCreateRequestWithDefaults call in other functions
				handleID := item.ClientHandle

				if uint32(len(g.tNodeList)) >= handleID {
					pullMessages, ack, err := g.ReadTriggerBatchPull(ctx, g.tNodeList[handleID], g.tNodeList[handleID].TNodeID.String())
					if err != nil {
						return msgs, ack, err
					}
					// g.log.Infof("Pull Messages: ", pullMessages)
					message := g.createMessageFromValue(item.Value.Value, g.tNodeList[handleID], g.tNodeList[handleID].TNodeID.String(), pullMessages)
					if message != nil {
						msgs = append(msgs, message)
					}
				}
			}
		default:
			g.log.Errorf("Unknown publish result %T", res.Value)
		}

		return msgs, func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			return nil
		}, nil

	case _, ok := <-ctx.Done():
		if !ok {
			g.log.Errorf("timeout channel was closed")
		} else {
			// Timeout occurred
			g.log.Error("Timeout waiting for response from g.subNotifyChan")
		}
		return nil, nil, errors.New("timeout waiting for response")
	}
}
