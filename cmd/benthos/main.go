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

package main

import (
	"context"

	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/ab_plugin"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/cal_mqtt"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/csv_plugin"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/influxdb"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/jsontosp"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/modbus_plugin"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/opcua_plugin"
	_ "github.com/GirishBhutiya/umh-redpanda-connect/plugins/s7comm_plugin"
	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func main() {
	service.RunCLI(context.Background())
}
