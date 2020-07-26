package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/golang/protobuf/proto"
	pcounter "github.com/synerex/proto_pcounter"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"log"
	"regexp"
	"sync"
	"time"
)

var (
	srcSrv             = flag.String("srcsrv", "127.0.0.1:9990", "Source Synerex Node ID Server")
	dstSrv             = flag.String("dstsrv", "127.0.0.1:9990", "Destination Synerex Node ID Server")
	counterFilter      = flag.String("counter", "", "Counter Filter")
	fillFilter         = flag.String("fillLevel", "", "fillLevel Filter")
	dwellTime          = flag.String("dwellTime", "", "dwellTime Filter")
	local              = flag.String("local", "", "Local Synerex Server")
	mu                 sync.Mutex
	version            = "0.01"
	sxSrcServerAddress string
	sxDstServerAddress string
	sxDstClient        *sxutil.SXServiceClient
	ctexp              *regexp.Regexp
	flexp              *regexp.Regexp
	dwexp              *regexp.Regexp
	msgCount           int64
)

func init() {
	msgCount = 0
}

func prepFilters() {
	if *counterFilter != "" {
		ctexp = regexp.MustCompile(*counterFilter)
	}
	if *fillFilter != "" {
		flexp = regexp.MustCompile(*fillFilter)
	}
	if *dwellTime != "" {
		dwexp = regexp.MustCompile(*dwellTime)
	}
}

func supplyPCounterCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	pc := &pcounter.PCounter{}

	err := proto.Unmarshal(sp.Cdata.Entity, pc)
	if err == nil {
		evts := make([]*pcounter.PEvent, 0, 1)
		for _, ev := range pc.Data {
			switch ev.Typ {
			case "counter":
				if ctexp.MatchString(pc.DeviceId) {
					evts = append(evts, ev)
				}
			case "fillLevel":
				if flexp.MatchString(pc.DeviceId) {
					evts = append(evts, ev)
				}
			case "dwellTime":
				if dwexp.MatchString(pc.DeviceId) {
					evts = append(evts, ev)
				}
			}
		}
		pc.Data = evts
		out, _ := proto.Marshal(pc)
		cont := api.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "PCounter",
			Cdata: &cont,
		}
		_, nerr := sxDstClient.NotifySupply(&smo)
		if nerr != nil {
			log.Printf("Send Fail!\n", nerr)
		} else {
			msgCount++
			//						log.Printf("Sent OK! %#v\n", pc)
		}
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxSrcServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxSrcServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxSrcServerAddress)
	}
	mu.Unlock()
}

func subscribePCounterSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyPCounterCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

// just for stat
func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(msgCount), fmt.Sprintf("recv:%d", msgCount))
		time.Sleep(time.Second * 3)
	}
}

func monitorStatusDst(dstNI *sxutil.NodeServInfo) {
	for {
		dstNI.SetNodeStatus(int32(msgCount), fmt.Sprintf("sent:%d", msgCount))
		time.Sleep(time.Second * 3)
	}
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("PCounter-Store(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	if *srcSrv == *dstSrv {
		log.Fatal("Input servers should not be same address")
	}

	prepFilters()

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	dstNI := sxutil.NewNodeServInfo() // for dst node server connection
	sxutil.RegisterDeferFunction(dstNI.UnRegisterNode)

	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC}
	// obtain synerex server address from nodeserv
	srcSSrv, err := sxutil.RegisterNode(*srcSrv, "PCFowardSrc", channelTypes, nil)

	if err != nil {
		log.Fatal("Can't register to source node...")
	}
	log.Printf("Connecting Source Server [%s]\n", srcSSrv)
	sxSrcServerAddress = srcSSrv

	dstSSrv, derr := dstNI.RegisterNodeWithCmd(*dstSrv, "PCFowardDst", channelTypes, nil, nil)
	if derr != nil {
		log.Fatal("Can't register to destination node...")
	}
	log.Printf("Connecting Destination Server [%s]\n", dstSSrv)
	sxDstServerAddress = dstSSrv

	wg := sync.WaitGroup{} // for syncing other goroutines
	srcClient := sxutil.GrpcConnectServer(sxSrcServerAddress)
	argJSON := fmt.Sprintf("{PCForwardSink}")
	sxSrclient := sxutil.NewSXServiceClient(srcClient, pbase.PEOPLE_COUNTER_SVC, argJSON)

	dstClient := sxutil.GrpcConnectServer(sxDstServerAddress)
	argDstJSON := fmt.Sprintf("{PCForwardSrc}")
	sxDstClient = sxutil.NewSXServiceClient(dstClient, pbase.PEOPLE_COUNTER_SVC, argDstJSON)

	wg.Add(1)

	go subscribePCounterSupply(sxSrclient)
	go monitorStatus()
	go monitorStatusDst(dstNI)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}
