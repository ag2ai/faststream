from dataclasses import dataclass

from faststream._internal.constants import EMPTY
from faststream._internal.subscriber.configs import SubscriberUseCaseConfigs
from faststream.middlewares.acknowledgement.conf import AckPolicy

__all__ = ("RedisSubscriberBaseConfigs",)


@dataclass
class RedisSubscriberBaseConfigs(SubscriberUseCaseConfigs):
    no_ack: bool

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.DO_NOTHING if self.no_ack else AckPolicy.REJECT_ON_ERROR
        return self._ack_policy

    @ack_policy.setter
    def ack_policy(self, policy: AckPolicy) -> None:
        self._ack_policy = policy
