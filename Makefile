# Copyright 2023 UMH Systems GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

all: clean target

clean:
	@rm -rf target tmp/bin tmp/benthos-*.zip

target:
	@mkdir -p tmp/bin
	@goreleaser build --single-target --snapshot --id benthos \
		--output ./tmp/bin/benthos
test:
	@go test -v ./...

lint:
	@golangci-lint run

format:
	@golangci-lint run --fix

.PHONY: clean target test lint format

allwindows: cleanwin targetwin

cleanwin:
	@cmd /C delwin.bat
	
targetwin:
	@mkdir -p tmp\bin
	@goreleaser build --single-target --snapshot --id benthos \
		--output tmp\bin\benthos.exe
buildwin:
	@goreleaser

dockerpush:
	docker buildx build  \
--tag unifactmanufacturinghub/benthos:0.0.3  \
--platform "linux/arm/v7,linux/arm64/v8,linux/arm64,linux/amd64"  \
--builder benthos-container  --push .

dockerpusharm:
	docker buildx build  \
--tag unifactmanufacturinghub/benthos:linuxarm0.9  \
--platform "linux/arm64"  \
--builder benthos-container  --push .

dockerpushamd:
	docker buildx build  \
--tag unifactmanufacturinghub/benthos:linuxamd0.6  \
--platform "linux/amd64"  \
--builder benthos-container  --push .

dockermanifest:
	docker manifest create \
unifactmanufacturinghub/benthos-umh:latest \
--amend unifactmanufacturinghub/benthos-linux386:latest \
--amend unifactmanufacturinghub/benthos-linuxamd64:latest \
--amend unifactmanufacturinghub/benthos-linuxarm64:latest

#dockerimgpull
#	docker pull --platform=linux/arm64 unifactmanufacturinghub/benthos:linuxarm