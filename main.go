package main

import (
	"flag"
	"net/http"
	"os"
	"strconv"

	"github.com/opensourceways/community-robot-lib/config"
	"github.com/opensourceways/community-robot-lib/interrupts"
	"github.com/opensourceways/community-robot-lib/logrusutil"
	"github.com/opensourceways/community-robot-lib/mq"
	liboptions "github.com/opensourceways/community-robot-lib/options"
	"github.com/opensourceways/community-robot-lib/utils"
	"github.com/sirupsen/logrus"
)

type options struct {
	service liboptions.ServiceOptions
}

func (o *options) Validate() error {
	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.Parse(args)

	return o
}

const component = "robot-gitee-access"

func main() {
	logrusutil.ComponentInit(component)

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err := o.Validate(); err != nil {
		logrus.WithError(err).Fatal("Invalid options")
	}

	configAgent := config.NewConfigAgent(func() config.Config {
		return new(configuration)
	})
	if err := configAgent.Start(o.service.ConfigFile); err != nil {
		logrus.WithError(err).Fatal("Error starting config agent.")
	}

	agent := demuxConfigAgent{agent: &configAgent, t: utils.NewTimer()}
	agent.start()

	d := dispatcher{
		agent: &agent,
	}

	if err := initMQ(configAgent); err != nil {
		logrus.WithError(err).Fatal("Error init broker.")
	}

	defer mq.Disconnect()

	subscriber, err := mq.Subscribe("gitee-webhook", handleGiteeMessage(&d), mq.Queue(component))
	if err != nil {
		logrus.WithError(err).Fatal("error subscribe gitee-webhook topic.")
	}

	defer subscriber.Unsubscribe()

	defer interrupts.WaitForGracefulShutdown()

	interrupts.OnInterrupt(func() {
		// agent depends on configAgent, so stop agent first.
		agent.stop()
		logrus.Info("demux stopped")

		configAgent.Stop()
		logrus.Info("config agent stopped")

		d.wait()
	})

	// Return 200 on / for health checks.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})

	httpServer := &http.Server{Addr: ":" + strconv.Itoa(o.service.Port)}

	interrupts.ListenAndServe(httpServer, o.service.GracePeriod)
}
