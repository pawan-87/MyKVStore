package server

import (
	"mykvstore/mykvstoreserverpb"

	"google.golang.org/grpc"
)

func (s *Server) Watch(g grpc.BidiStreamingServer[mykvstoreserverpb.WatchRequest, mykvstoreserverpb.WatchResponse]) error {
	//TODO implement me
	panic("implement me")
}
