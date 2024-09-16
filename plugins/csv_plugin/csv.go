package csv_plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type CSVOutput struct {
	FTPHost     string
	FTPUsername string
	FTPPassword string
	FTPPort     int
	Path        string
	File        string
	SourceType  int
	NetworkPath string
	node        nodeDef
	conn        *ftp.ServerConn
	timeout     time.Duration
	TotalRow    int
	log         *service.Logger
}
type nodeDef struct {
	ID        int
	Name      string
	Group     string
	DB        string
	Historian string
	SqlSp     string
	DataType  string
}

var CSVConfigSpec = service.NewConfigSpec().
	Summary("Creates an CSV output").
	Field(service.NewStringField("ftphost").Description("Host address to connect FTP")).
	Field(service.NewStringField("ftpusername").Description("Username for FTP access. If not set, no username is used.").Default("anonymous")).
	Field(service.NewStringField("ftppassword").Description("Password for FTP access. If not set, no password is used.").Default("anonymous")).
	Field(service.NewIntField("ftpport").Description("Port to connect FTP").Default(21)).
	Field(service.NewStringField("path").Description("path from the FTP root.")).
	Field(service.NewStringField("file").Description("list of all files")).
	Field(service.NewStringField("sourcetype").Description("sourcetype is FTP or sharedfolder. If not set, sharedfolder is used.").Default("sharedfolder")).
	Field(service.NewStringField("networkpath").Description("networkpath for local path access.")).
	Field(service.NewStringField("node").Description("List of nodes like DB,group etc")).
	Field(service.NewIntField("timeout").Description("The timeout duration in seconds for connection attempts and read requests.").Default(10))

func newCSVoutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	ftpHost, err := conf.FieldString("ftphost")
	if err != nil {
		return nil, err
	}
	ftpUsername, err := conf.FieldString("ftpusername")
	if err != nil {
		return nil, err
	}
	ftpPassword, err := conf.FieldString("ftppassword")
	if err != nil {
		return nil, err
	}
	ftpPort, err := conf.FieldInt("ftpport")
	if err != nil {
		return nil, err
	}
	path, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}
	timeoutInt, err := conf.FieldInt("timeout")
	if err != nil {
		return nil, err
	}
	file, err := conf.FieldString("file")
	if err != nil {
		return nil, err
	}
	sourcetypestr, err := conf.FieldString("sourcetype")
	if err != nil {
		return nil, err
	}
	networkpath, err := conf.FieldString("networkpath")
	if err != nil {
		return nil, err
	}
	node, err := conf.FieldString("node")
	if err != nil {
		return nil, err
	}
	/* if len(file) != len(node) {
		return nil, errors.New("the number of files must match the number of nodes")
	} */
	m := &CSVOutput{
		Path:        path,
		FTPHost:     ftpHost,
		FTPUsername: ftpUsername,
		FTPPassword: ftpPassword,
		FTPPort:     ftpPort,
		File:        file,
		SourceType:  convertSourceType(sourcetypestr),
		NetworkPath: networkpath,
		node:        ParseNodeDef(node),
		log:         mgr.Logger(),
		timeout:     time.Duration(timeoutInt) * time.Second,
	}

	return service.AutoRetryNacksBatched(m), nil
}

func init() {
	// Register our new output plugin
	err := service.RegisterBatchInput(
		"csv", CSVConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			mgr.Logger().Infof("Created & maintained by the BGRI ")
			return newCSVoutput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}
func (g *CSVOutput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {

	msgs := service.MessageBatch{}

	if g.SourceType == ST_FTP {
		ftpReader, err := g.conn.Retr(g.Path + g.File)
		if err != nil {
			return nil, func(ctx context.Context, err error) error {
				return nil // Acknowledgment handling here if needed
			}, err
		}
		line, length, err := getLastLineFromFTPFile(ftpReader)
		if err != nil {
			return nil, func(ctx context.Context, err error) error {
				return nil // Acknowledgment handling here if needed
			}, err
		}
		if g.TotalRow < length {
			msgs = append(msgs, g.createMessageFromValue(line, g.node))
			g.TotalRow = length
		}
	} else {
		line, length, err := getLastLineFromAbsPath(g.Path + g.File)
		if err != nil {
			return nil, func(ctx context.Context, err error) error {
				return nil // Acknowledgment handling here if needed
			}, err
		}
		if g.TotalRow < length {
			msgs = append(msgs, g.createMessageFromValue(line, g.node))
			g.TotalRow = length
		}
	}

	//log.Println(line)

	return msgs, func(ctx context.Context, err error) error {
		return nil // Acknowledgment handling here if needed
	}, nil
}
func (g *CSVOutput) createMessageFromValue(lastline map[string]string, node nodeDef) *service.Message {
	line, err := json.Marshal(lastline)
	if err != nil {
		g.log.Error(err.Error())
		return nil
	}
	//log.Println(string(line))
	message := service.NewMessage(line)

	message.MetaSet("db", node.DB)
	message.MetaSet("name", node.Name)
	message.MetaSet("group", node.Group)
	message.MetaSet("historian", node.Historian)
	message.MetaSet("sqlSp", node.SqlSp)
	//message.MetaSet("value", lastline)

	return message
}

func (g *CSVOutput) Connect(ctx context.Context) error {
	g.TotalRow = 0
	if g.SourceType == ST_FTP {
		c, err := ftp.Dial(fmt.Sprintf("%s:%d", g.FTPHost, g.FTPPort), ftp.DialWithTimeout(g.timeout))
		if err != nil {
			g.log.Error(err.Error())
			//log.Fatal(err)
			return err
		}

		err = c.Login(g.FTPUsername, g.FTPPassword)
		if err != nil {
			g.log.Error(err.Error())
			//log.Fatal(err)
			return err
		}

		g.conn = c
	}

	return nil
}
func (g *CSVOutput) Close(ctx context.Context) error {
	if g.SourceType == ST_FTP {
		if err := g.conn.Quit(); err != nil {
			g.log.Error(err.Error())
			return err
		}
	}

	return nil
}
