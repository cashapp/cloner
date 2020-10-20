package clone

import "google.golang.org/grpc/grpclog"

func init() {
	// just make it shut up for now
	grpclog.SetLoggerV2(grpcLoggger{})
}

type grpcLoggger struct {
}

func (g grpcLoggger) Info(args ...interface{}) {

}

func (g grpcLoggger) Infoln(args ...interface{}) {

}

func (g grpcLoggger) Infof(format string, args ...interface{}) {

}

func (g grpcLoggger) Warning(args ...interface{}) {

}

func (g grpcLoggger) Warningln(args ...interface{}) {

}

func (g grpcLoggger) Warningf(format string, args ...interface{}) {

}

func (g grpcLoggger) Error(args ...interface{}) {

}

func (g grpcLoggger) Errorln(args ...interface{}) {

}

func (g grpcLoggger) Errorf(format string, args ...interface{}) {

}

func (g grpcLoggger) Fatal(args ...interface{}) {

}

func (g grpcLoggger) Fatalln(args ...interface{}) {

}

func (g grpcLoggger) Fatalf(format string, args ...interface{}) {

}

func (g grpcLoggger) V(l int) bool {
	return false
}
