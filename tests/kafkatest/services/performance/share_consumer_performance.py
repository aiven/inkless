# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os

from kafkatest.services.kafka.util import fix_opts_for_new_jvm, get_log4j_config_param, get_log4j_config_for_tools
from kafkatest.services.performance import PerformanceService
from kafkatest.version import DEV_BRANCH


class ShareConsumerPerformanceService(PerformanceService):
    """
        See ShareConsumerPerformance tool as the source of truth on these settings, but for reference:

        "topic", "REQUIRED: The topic to consume from."

        "group", "The group id to consume on."

        "fetch-size", "The amount of data to fetch in a single request."

        "socket-buffer-size", "The size of the tcp RECV size."

        "consumer.config", "Consumer config properties file."
    """

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/share_consumer_performance"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "share_consumer_performance.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "share_consumer_performance.stderr")
    LOG_FILE = os.path.join(LOG_DIR, "share_consumer_performance.log")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "share_consumer.properties")

    logs = {
        "share_consumer_performance_output": {
            "path": STDOUT_CAPTURE,
            "collect_default": True},
        "share_consumer_performance_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": True},
        "share_consumer_performance_log": {
            "path": LOG_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, messages, group="perf-share-consumer", version=DEV_BRANCH, timeout=10000, settings={}):
        super(ShareConsumerPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.security_config = kafka.security_config.client_config()
        self.topic = topic
        self.messages = messages
        self.settings = settings
        self.group = group
        self.timeout = timeout

        # These less-frequently used settings can be updated manually after instantiation
        self.fetch_size = None
        self.socket_buffer_size = None

        for node in self.nodes:
            node.version = version

    def args(self):
        """Dictionary of arguments used to start the Share Consumer Performance script."""
        args = {
            'topic': self.topic,
            'messages': self.messages,
            'bootstrap-server': self.kafka.bootstrap_servers(self.security_config.security_protocol),
            'group': self.group,
            'timeout': self.timeout
        }

        if self.fetch_size is not None:
            args['fetch-size'] = self.fetch_size

        if self.socket_buffer_size is not None:
            args['socket-buffer-size'] = self.socket_buffer_size

        return args

    def start_cmd(self, node):
        cmd = fix_opts_for_new_jvm(node)
        cmd += "export LOG_DIR=%s;" % ShareConsumerPerformanceService.LOG_DIR
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        cmd += " export KAFKA_LOG4J_OPTS=\"%s%s\";" % (get_log4j_config_param(node), get_log4j_config_for_tools(node))
        cmd += " %s" % self.path.script("kafka-share-consumer-perf-test.sh", node)
        for key, value in self.args().items():
            cmd += " --%s %s" % (key, value)

        cmd += " --consumer.config %s" % ShareConsumerPerformanceService.CONFIG_FILE

        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        cmd += " 2>> %(stderr)s | tee -a %(stdout)s" % {'stdout': ShareConsumerPerformanceService.STDOUT_CAPTURE,
                                                        'stderr': ShareConsumerPerformanceService.STDERR_CAPTURE}
        return cmd

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % ShareConsumerPerformanceService.PERSISTENT_ROOT, allow_fail=False)

        log_config = self.render(get_log4j_config_for_tools(node), log_file=ShareConsumerPerformanceService.LOG_FILE)
        node.account.create_file(get_log4j_config_for_tools(node), log_config)
        node.account.create_file(ShareConsumerPerformanceService.CONFIG_FILE, str(self.security_config))
        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("Share consumer performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            last = line

        # Parse and save the last line's information
        if last is not None:
            parts = last.split(',')
            self.results[idx-1] = {
                'total_mb': float(parts[2]),
                'mbps': float(parts[3]),
                'records_per_sec': float(parts[5]),
            }
