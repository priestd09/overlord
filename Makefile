test-create:
	go test -v overlord/job/create

build:
	cd cmd/apiserver && go build && cd -
	cd cmd/scheduler && go build && cd -
	cd cmd/scheduler && go build && cd -
