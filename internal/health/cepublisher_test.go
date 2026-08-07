package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	v1alpha1 "github.com/dcm-project/environment-agent/api/v1alpha1"
	"github.com/dcm-project/environment-agent/internal/cloudevent"
	"github.com/dcm-project/environment-agent/internal/health"
	"github.com/dcm-project/environment-agent/internal/provider/store"
	"github.com/dcm-project/environment-agent/internal/routing"
)

type fakeStore struct {
	provider *store.StoredProvider
	err      error
}

func (f *fakeStore) GetByID(_ context.Context, _ string) (*store.StoredProvider, error) {
	return f.provider, f.err
}

type fakePublisher struct {
	published []publishedMsg
}

type publishedMsg struct {
	subject string
	data    []byte
}

func (f *fakePublisher) Publish(_ context.Context, subject string, data []byte) error {
	f.published = append(f.published, publishedMsg{subject: subject, data: data})
	return nil
}

func (f *fakePublisher) PublishWithMsgID(_ context.Context, subject, _ string, data []byte) error {
	f.published = append(f.published, publishedMsg{subject: subject, data: data})
	return nil
}

var _ = Describe("CEPublisher", func() {
	var (
		logger *slog.Logger
		fs     *fakeStore
		pub    *fakePublisher
		cePub  *health.CEPublisher
	)

	BeforeEach(func() {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
		fs = &fakeStore{
			provider: &store.StoredProvider{
				ID:          "provider-1",
				Name:        "my-sp",
				ServiceType: "compute",
			},
		}
		pub = &fakePublisher{}
		cePub = health.NewCEPublisher(fs, pub, logger, "agent-1", "topic-main")
	})

	It("publishes a degraded CE on transition to Unhealthy", func() {
		cePub.OnTransition(context.Background(), "provider-1", v1alpha1.Ready, v1alpha1.Unhealthy)

		Expect(pub.published).To(HaveLen(1))
		Expect(pub.published[0].subject).To(Equal(cloudevent.SubjectHealth))

		var event cloudevents.Event
		Expect(json.Unmarshal(pub.published[0].data, &event)).To(Succeed())
		Expect(event.Type()).To(Equal(cloudevent.TypeHealthDegraded))

		var data routing.HealthEventData
		Expect(json.Unmarshal(event.Data(), &data)).To(Succeed())
		Expect(data.ServiceType).To(Equal("compute"))
		Expect(data.AffectedProvider).To(Equal("my-sp"))
		Expect(data.AgentName).To(Equal("agent-1"))
		Expect(data.TopicName).To(Equal("topic-main"))
		Expect(data.Reason).To(ContainSubstring("threshold"))
	})

	It("publishes an unavailable CE on transition to Unavailable", func() {
		cePub.OnTransition(context.Background(), "provider-1", v1alpha1.Unhealthy, v1alpha1.Unavailable)

		Expect(pub.published).To(HaveLen(1))
		var event cloudevents.Event
		Expect(json.Unmarshal(pub.published[0].data, &event)).To(Succeed())
		Expect(event.Type()).To(Equal(cloudevent.TypeHealthUnavailable))

		var data routing.HealthEventData
		Expect(json.Unmarshal(event.Data(), &data)).To(Succeed())
		Expect(data.Reason).To(ContainSubstring("unavailable"))
	})

	It("does not publish on transition to Available", func() {
		cePub.OnTransition(context.Background(), "provider-1", v1alpha1.Unhealthy, v1alpha1.Ready)
		Expect(pub.published).To(BeEmpty())
	})

	It("does not publish when store lookup fails", func() {
		fs.err = errors.New("store broken")
		fs.provider = nil

		cePub.OnTransition(context.Background(), "provider-1", v1alpha1.Ready, v1alpha1.Unhealthy)
		Expect(pub.published).To(BeEmpty())
	})
})
