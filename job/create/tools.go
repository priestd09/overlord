package create

import (
	"context"
	"fmt"
	"overlord/lib/etcd"
)

func cleanEtcdDirtyDir(ctx context.Context, e *etcd.Etcd, instance string) error {
	return e.RMDir(ctx, fmt.Sprintf("%s/%s", etcd.InstanceDirPrefix, instance))
}

func genAgentPortSequence(ctx context.Context, e *etcd.Etcd, host string) (int64, error) {
	return e.Sequence(ctx, fmt.Sprintf("%s/%s", etcd.AgentPortSequence, host))
}
