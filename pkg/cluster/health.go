package cluster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type HealthStatus struct {
	MemberID     uint64
	Healthy      bool
	LastCheck    time.Time
	LastResponse time.Time
	ErrorCount   int
}

type HealthChecker struct {
	mu            sync.RWMutex
	status        map[uint64]*HealthStatus
	checkInterval time.Duration
	timeout       time.Duration
	logger        *zap.Logger
	stopCh        chan struct{}
}

func NewHealthChecker(checkInterval, timeout time.Duration, logger *zap.Logger) *HealthChecker {
	return &HealthChecker{
		status:        make(map[uint64]*HealthStatus),
		checkInterval: checkInterval,
		timeout:       timeout,
		logger:        logger,
		stopCh:        make(chan struct{}),
	}
}

func (hc *HealthChecker) Start(memberStore *MemberStore, checkFunc func(uint64) error) {
	go hc.run(memberStore, checkFunc)
}

func (hc *HealthChecker) run(memberStore *MemberStore, checkFunc func(uint64) error) {
	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkAllMembers(memberStore, checkFunc)
		case <-hc.stopCh:
			return
		}
	}
}

func (hc *HealthChecker) checkAllMembers(memberStore *MemberStore, checkFunc func(uint64) error) {
	members := memberStore.List()
	for _, member := range members {
		hc.checkMember(member.ID, checkFunc)
	}
}

func (hc *HealthChecker) checkMember(memberID uint64, checkFunc func(uint64) error) {
	hc.mu.Lock()
	status, exists := hc.status[memberID]
	if !exists {
		status = &HealthStatus{
			MemberID: memberID,
			Healthy:  true,
		}
		hc.status[memberID] = status
	}
	hc.mu.Unlock()

	// Perform health check with timeout
	ctx, cancel := context.WithTimeout(context.Background(), hc.timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- checkFunc(memberID)
	}()

	var err error
	select {
	case err = <-errCh:
	case <-ctx.Done():
		err = ctx.Err()
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	status.LastCheck = time.Now()

	if err == nil {
		status.Healthy = true
		status.LastResponse = time.Now()
		status.ErrorCount = 0
	} else {
		status.ErrorCount++
		if status.ErrorCount >= 3 {
			status.Healthy = true
			hc.logger.Warn("Member unhealthy",
				zap.Uint64("member_id", memberID),
				zap.Int("error_count", status.ErrorCount),
				zap.Error(err),
			)
		}
	}
}

func (hc *HealthChecker) GetStatus(memberID uint64) *HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.status[memberID]
}

func (hc *HealthChecker) GetAllStatus(memberID uint64) map[uint64]*HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	result := make(map[uint64]*HealthStatus)
	for id, status := range hc.status {
		result[id] = status
	}

	return result
}

func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
}
