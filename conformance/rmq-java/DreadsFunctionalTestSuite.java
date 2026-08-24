// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// This software, the RabbitMQ Java client library, is triple-licensed under the
// Mozilla Public License 2.0 ("MPL"), the GNU General Public License version 2
// ("GPL") and the Apache License version 2 ("ASL"). For the MPL, please see
// LICENSE-MPL-RabbitMQ. For the GPL, please see LICENSE-GPL2.  For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.


package com.rabbitmq.client.test.functional;

// ---------------------------------------------------------------------------
// dreads laundered suite: the official FunctionalTestSuite MINUS the classes
// that drive the broker through rabbitmqctl / Host (management plane, not the
// AMQP 0-9-1 wire protocol). Excluded, each with its Host dependency:
//   Policies                  - Host.rabbitmqctl set_policy
//   ConnectionRecovery        - Host.closeConnection (server-side conn kill)
//   TopologyRecoveryFiltering - Host-driven recovery orchestration
//   TopologyRecoveryRetry     - Host.closeAllConnections
//   UserIDHeader              - Host.rabbitmqctl user management
// Everything else is wire-level conformance and runs against dreads as-is:
//   ./mvnw verify -Dit.test=DreadsFunctionalTestSuite -Drabbitmqctl.bin=/bin/false
// ---------------------------------------------------------------------------


import com.rabbitmq.client.impl.WorkPoolTests;
import com.rabbitmq.client.test.Bug20004Test;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
		ConnectionOpen.class,
		Heartbeat.class,
		Tables.class,
		DoubleDeletion.class,
		Routing.class,
		BindingLifecycle.class,
		Recover.class,
		Reject.class,
		Transactions.class,
		RequeueOnConnectionClose.class,
		RequeueOnChannelClose.class,
		DurableOnTransient.class,
		NoRequeueOnCancel.class,
		Bug20004Test.class,
		ExchangeDeleteIfUnused.class,
		QosTests.class,
		AlternateExchange.class,
		ExchangeExchangeBindings.class,
		ExchangeExchangeBindingsAutoDelete.class,
		ExchangeDeclare.class,
		FrameMax.class,
		QueueLifecycle.class,
		QueueLease.class,
		QueueExclusivity.class,
		QueueSizeLimit.class,
		InvalidAcks.class,
		InvalidAcksTx.class,
		DefaultExchange.class,
		UnbindAutoDeleteExchange.class,
		Confirm.class,
		ConsumerNotifications.class,
		UnexpectedFrames.class,
		PerQueueTTL.class,
		PerMessageTTL.class,
		PerQueueVsPerMessageTTL.class,
		DeadLetterExchange.class,
		SaslMechanisms.class,
		InternalExchange.class,
		CcRoutes.class,
		WorkPoolTests.class,
		HeadersExchangeValidation.class,
		ConsumerPriorities.class,
		ExceptionHandling.class,
		PerConsumerPrefetch.class,
		DirectReplyTo.class,
		ConsumerCount.class,
		BasicGet.class,
		Nack.class,
		ExceptionMessages.class,
		Metrics.class,
		MicrometerObservationCollectorMetrics.class,
		TopologyRecoveryRetry.class
})
public class DreadsFunctionalTestSuite {

}
