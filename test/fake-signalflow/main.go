package main

import (
	"log"
	"net/http"
	"os"

	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	"github.com/signalfx/signalfx-go/idtool"
)

const program = "data('demo').publish()"

func main() {
	fake := signalflow.NewRunningFakeBackend()
	defer fake.Stop()
	fake.AddProgramTSIDs(program, []idtool.ID{idtool.ID(1)})
	fake.SetTSIDFloatData(idtool.ID(1), 42)

	address := os.Getenv("LISTEN_ADDR")
	if address == "" {
		address = ":8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", fake)

	log.Fatal(http.ListenAndServe(address, mux))
}
