# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from charms.leadership import leader_get
from charms.reactive import RelationBase, hook, scopes


class ZookeeperPeers(RelationBase):
    scope = scopes.UNIT

    @hook('{peers:zookeeper-quorum}-relation-joined')
    def joined(self):
        conv = self.conversation()
        conv.remove_state('{relation_name}.departed')
        conv.set_state('{relation_name}.joined')

    @hook('{peers:zookeeper-quorum}-relation-departed')
    def departed(self):
        conv = self.conversation()
        conv.remove_state('{relation_name}.joined')
        conv.set_state('{relation_name}.departed')

    @hook('{peers:zookeeper-quorum}-relation-changed')
    def changed(self):
        conv = self.conversation()
        conv.set_state('{relation_name}.changed')

    def dismiss_departed(self):
        for conv in self.conversations():
            conv.remove_state('{relation_name}.departed')

    def dismiss_joined(self):
        for conv in self.conversations():
            conv.remove_state('{relation_name}.joined')

    def dismiss_changed(self):
        for conv in self.conversations():
            conv.remove_state('{relation_name}.changed')

    def get_nodes(self):
        nodes = []
        for conv in self.conversations():
            nodes.append((conv.scope, conv.get_remote('private-address')))

        return nodes

    def restarted_nodes(self):
        nodes = []
        nonce = leader_get('restart_nonce')
        if not nonce:
            return nodes  # We're not restarting if no nonce is set.
        for conv in self.conversations():
            if conv.get_remote('restarted.{}'.format(nonce)):
                nodes.append((conv.scope, conv.get_remote('private-address')))

        return nodes

    def set_zk_leader(self):
        '''
        Inform peers that the unit that calls this method is the Zookeeper leader.

        Note that Zookeeper tracks leadership separately from juju;
        the Zookeeper leader is not necessarily the Juju leader.

        '''
        for conv in self.conversations():
            conv.set_remote('is_zk_leader', True)

    def find_zk_leader(self):
        '''
        Find the private address of the leader.

        '''
        for conv in self.conversations():
            if conv.get_remote('is_zk_leader'):
                return conv.get_remote('private_address')

    def inform_restart(self):
        '''
        Inform our peers that we have restarted, usually as part of a
        rolling restart.

        '''
        for conv in self.conversations():
            nonce = leader_get('restart_nonce')
            conv.set_remote('restarted.{}'.format(nonce), json.dumps(True))
