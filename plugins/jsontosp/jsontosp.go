package jsontosp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

var JSONObjectSpec = bloblang.NewPluginSpec().
	Param(bloblang.NewStringParam("jsonstr"))

func init() {

	err := bloblang.RegisterFunctionV2(
		"jsontosp", JSONObjectSpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			jsonstr, err := args.GetString("jsonstr")
			if err != nil {
				return nil, err
			}
			return func() (interface{}, error) {
				var obj map[string]any
				err := json.Unmarshal([]byte(jsonstr), &obj)
				if err != nil {
					return nil, err
				}

				var sb strings.Builder
				for key, value := range obj {
					if value != nil {
						switch reflect.ValueOf(value).Kind() {
						case reflect.String:
							sb.WriteString(fmt.Sprintf("%s%s='%s',", "@", key, value))
						case reflect.Float64, reflect.Float32:
							sb.WriteString(fmt.Sprintf("%s%s=%f,", "@", key, value))
						case reflect.Bool:
							sb.WriteString(fmt.Sprintf("%s%s=%t,", "@", key, value))
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							sb.WriteString(fmt.Sprintf("%s%s=%d,", "@", key, value))
						}
					}

					/* v, err := strconv.ParseFloat(value.(string), 64)
					if err == nil {

						continue
					}
					b, err := strconv.ParseBool(value.(string))
					if err == nil {
						sb.WriteString(fmt.Sprintf("%s%s=%t,", "@", key, b))
						continue
					}
					sb.WriteString(fmt.Sprintf("%s%s='%s',", "@", key, value)) */
				}
				return sb.String(), nil
			}, nil
		})
	if err != nil {
		panic(err)
	}
}
