package plugin

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// InfluxDBOutput represents the configuration for the InfluxDB output plugin.
type InfluxDBOutput struct {
	token     string
	endpoint  string
	org       string
	bucket    string
	precision string
	username  string
	password  string
	client    influxdb2.Client
	log       *service.Logger
}

var InfluxDBConfigSpec = service.NewConfigSpec().
	Summary("Creates an influx DB output").
	Field(service.NewStringField("endpoint").Description("Address of the Influx DB server to connect with.")).
	Field(service.NewStringField("username").Description("Username for server access. If not set, no username is used.").Default("admin")).
	Field(service.NewStringField("password").Description("Password for server access. If not set, no password is used.").Default("admin123")).
	Field(service.NewStringField("token").Description("Token")).
	Field(service.NewStringField("org").Description("Organisation")).
	Field(service.NewStringField("bucket").Description("Bucket")).
	Field(service.NewStringField("precision").Description("Precision").Default(""))

func newInfluxDBOutput(conf *service.ParsedConfig, mgr *service.Resources) (*InfluxDBOutput, int, error) {
	endpoint, err := conf.FieldString("endpoint")
	if err != nil {
		return nil, 1, err
	}

	username, err := conf.FieldString("username")
	if err != nil {
		return nil, 1, err
	}

	password, err := conf.FieldString("password")
	if err != nil {
		return nil, 1, err
	}

	token, err := conf.FieldString("token")
	if err != nil {
		return nil, 1, err
	}

	org, err := conf.FieldString("org")
	if err != nil {
		return nil, 1, err
	}

	bucket, err := conf.FieldString("bucket")
	if err != nil {
		return nil, 1, err
	}

	precision, err := conf.FieldString("precision")
	if err != nil {
		return nil, 1, err
	}

	return &InfluxDBOutput{
		endpoint:  endpoint,
		username:  username,
		password:  password,
		token:     token,
		org:       org,
		precision: precision,
		bucket:    bucket,
		log:       mgr.Logger(),
	}, 1, nil

}

// Connect establishes a connection to InfluxDB.
func (i *InfluxDBOutput) Connect(ctx context.Context) error {
	client := influxdb2.NewClient(i.endpoint, i.token)
	i.client = client
	i.log.Infof("Connected to influx DB", client)
	return nil
}

// Close closes the InfluxDB connection.
func (i *InfluxDBOutput) Close(ctx context.Context) error {
	i.client.Close()
	return nil
}

func (i *InfluxDBOutput) Write(ctx context.Context, msg *service.Message) error {
	content, err := msg.AsBytes()
	if err != nil {
		return err
	}

	currMsg := make(map[string]interface{})
	err = json.Unmarshal([]byte(content), &currMsg)
	if err != nil {
		log.Println("currMsg:", currMsg)
		return err
	}
	fields := make(map[string]interface{})
	tags := make(map[string]string)

	group := currMsg["group"]
	if group != nil {
		tags["group"] = group.(string)
	}
	datatype := currMsg["datatype"]
	if datatype != nil {
		tags["datatype"] = datatype.(string)
	}

	for key, val := range currMsg {

		if val == nil {
			continue
		}

		if floatValue, ok := val.(float64); ok {
			fields[key] = floatValue
		} else {
			switch reflect.ValueOf(val).Kind() {
			case reflect.Bool:
				boolValue := val.(bool)

				if boolValue {
					fields[key] = 1.0
				} else {
					fields[key] = 0.0
				}
			case reflect.String:
				if strings.Contains(val.(string), "true") {
					fields[key] = 1.0
				} else if strings.Contains(val.(string), "false") {
					fields[key] = 0.0
				}
			case reflect.Float64:
				fields[key] = val.(float64)
			}

			/* boolValue, err := strconv.ParseBool(val.(string))
			if err == nil {

				if boolValue {
					fields[key] = 1
				} else {
					fields[key] = 0
				}
				//log.Println("key:", key, " value:", fields[key], " datatype:", tags["datatype"], " booltype:", boolValue)
			}
			if strings.Contains(val.(string), "true") {
				fields[key] = 1
			} else if strings.Contains(val.(string), "false") {
				fields[key] = 0
			}
			if err != nil {
				log.Println("val:", val)
				return err
			}
			floatValue, err := strconv.ParseFloat(val.(string), 64)
			if err != nil {
				log.Println("err:", err)
				continue
			} else {
				fields[key] = floatValue
			} */
		}
		//log.Println("key:", key, " val:", val)
	}

	writeAPI := i.client.WriteAPIBlocking(i.org, i.bucket)

	p := influxdb2.NewPoint(i.bucket, tags, fields, time.Now())

	// Write point immediately
	writeAPI.WritePoint(context.Background(), p)
	return nil
}

func init() {
	// Register our new output plugin
	err := service.RegisterOutput(
		"influxdb", InfluxDBConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return newInfluxDBOutput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}
