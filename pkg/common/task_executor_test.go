package common

import (
	"testing"

	concurrent "github.com/orcaman/concurrent-map"
)

func Test_taskExecutor_execute(t *testing.T) {
	type fields struct {
		//RWMutex      sync.RWMutex
		runningTasks concurrent.ConcurrentMap
		updatedTasks Queue
		closed       bool
	}
	type args struct {
		tw *TaskWrapper
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
		{
			"NormalTask",
			fields{runningTasks: concurrent.New(), updatedTasks: *NewMutexQueue()},
			args{
				tw: &TaskWrapper{
					KlevrTask: &KlevrTask{
						ID:             1,
						ZoneID:         3,
						Name:           "Hello-Command",
						TaskType:       TaskType(AtOnce),
						AgentKey:       "agent-1234",
						TotalStepCount: 1,
						Parameter:      "",
						Steps: []*KlevrTaskStep{
							&KlevrTaskStep{
								ID:          0,
								Seq:         1,
								CommandName: "Hello",
								CommandType: CommandType(INLINE),
								Command:     `echo "hello" >> ~/hello.txt`,
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &taskExecutor{
				//RWMutex:      tt.fields.RWMutex,
				runningTasks: tt.fields.runningTasks,
				updatedTasks: tt.fields.updatedTasks,
				closed:       tt.fields.closed,
			}
			executor.execute(tt.args.tw)
		})
	}
}
