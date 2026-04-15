/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function () {
  'use strict';

  const globalErr = document.getElementById('globalErr');

  function setErr(msg) {
    globalErr.textContent = msg || '';
  }

  async function api(method, path, bodyObj) {
    const opts = {
      method,
      credentials: 'same-origin',
      headers: {}
    };
    if (bodyObj !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(bodyObj);
    }
    const res = await fetch(path, opts);
    let data = null;
    const text = await res.text();
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (e) {
        throw new Error('Invalid JSON: ' + text.slice(0, 200));
      }
    }
    if (!res.ok) {
      const msg = (data && data.error) ? data.error : ('HTTP ' + res.status);
      throw new Error(msg);
    }
    if (data && data.error) {
      throw new Error(data.error);
    }
    return data;
  }

  function parseClientProps() {
    const raw = document.getElementById('clientProps').value.trim();
    if (!raw) {
      return {};
    }
    return JSON.parse(raw);
  }

  document.getElementById('btnConnect').addEventListener('click', async () => {
    setErr('');
    const bootstrap = document.getElementById('bootstrap').value.trim();
    let clientProps = {};
    try {
      clientProps = parseClientProps();
    } catch (e) {
      setErr('client properties must be valid JSON: ' + e.message);
      return;
    }
    try {
      await api('POST', '/api/session', { bootstrapServers: bootstrap, clientProps });
      document.getElementById('connMsg').textContent = 'Connected.';
      document.getElementById('connMsg').className = 'ok';
    } catch (e) {
      document.getElementById('connMsg').textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnDisconnect').addEventListener('click', async () => {
    setErr('');
    try {
      await api('DELETE', '/api/session');
      document.getElementById('connMsg').textContent = 'Disconnected.';
      document.getElementById('connMsg').className = 'muted';
    } catch (e) {
      setErr(e.message);
    }
  });

  document.getElementById('btnConnStatus').addEventListener('click', async () => {
    setErr('');
    try {
      const s = await api('GET', '/api/session');
      const msg = s.connected ? 'Connected.' : 'Not connected.';
      document.getElementById('connMsg').textContent = msg;
      document.getElementById('connMsg').className = s.connected ? 'ok' : 'muted';
    } catch (e) {
      setErr(e.message);
    }
  });

  function renderTopicList(names) {
    const ul = document.getElementById('topicList');
    ul.innerHTML = '';
    names.forEach((n) => {
      const li = document.createElement('li');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.dataset.name = n;
      li.appendChild(cb);
      li.appendChild(document.createTextNode(n));
      ul.appendChild(li);
    });
  }

  document.getElementById('btnListTopics').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('topicMsg');
    try {
      const data = await api('GET', '/api/topics');
      renderTopicList(data.topics || []);
      msg.textContent = 'Listed ' + (data.topics ? data.topics.length : 0) + ' topics.';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnCreateTopic').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('topicMsg');
    const name = document.getElementById('newTopicName').value.trim();
    const partitions = parseInt(document.getElementById('newTopicParts').value, 10);
    const replicationFactor = parseInt(document.getElementById('newTopicRf').value, 10);
    try {
      await api('POST', '/api/topics', { name, partitions, replicationFactor });
      msg.textContent = 'Topic created (or already exists).';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnDeleteTopics').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('topicMsg');
    const manual = document.getElementById('deleteTopicNames').value.split(',').map((s) => s.trim()).filter(Boolean);
    const fromList = [];
    document.querySelectorAll('#topicList input[type=checkbox]:checked').forEach((cb) => {
      fromList.push(cb.dataset.name);
    });
    const names = manual.length ? manual : fromList;
    if (!names.length) {
      setErr('Enter comma-separated topic names or select topics in the list.');
      return;
    }
    try {
      await api('POST', '/api/topics/delete', { names });
      msg.textContent = 'Delete requested.';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });

  let vcNamesCache = [];

  function renderVcList(names) {
    vcNamesCache = names;
    const ul = document.getElementById('vcList');
    ul.innerHTML = '';
    names.forEach((n) => {
      const li = document.createElement('li');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.dataset.name = n;
      li.appendChild(cb);
      li.appendChild(document.createTextNode(n));
      ul.appendChild(li);
    });
  }

  document.getElementById('btnListVc').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('vcMsg');
    try {
      const data = await api('GET', '/api/virtual-clusters');
      const names = data.virtualClusters || [];
      renderVcList(names);
      msg.textContent = 'Listed ' + names.length + ' virtual clusters.';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnDescribeVc').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('vcMsg');
    const pre = document.getElementById('vcDescribe');
    const selected = [];
    document.querySelectorAll('#vcList input[type=checkbox]:checked').forEach((cb) => {
      selected.push(cb.dataset.name);
    });
    const names = selected.length ? selected : vcNamesCache;
    if (!names.length) {
      setErr('List virtual clusters first, or select names in the list.');
      return;
    }
    try {
      const data = await api('POST', '/api/virtual-clusters/describe', { names });
      pre.hidden = false;
      pre.textContent = JSON.stringify(data.descriptions, null, 2);
      msg.textContent = 'Described ' + names.length + ' virtual cluster(s).';
      msg.className = 'ok';
    } catch (e) {
      pre.hidden = true;
      msg.textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnCreateVc').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('vcMsg');
    const name = document.getElementById('newVcName').value.trim();
    try {
      await api('POST', '/api/virtual-clusters', { name });
      msg.textContent = 'Virtual cluster created.';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnDeleteVc').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('vcMsg');
    const manual = document.getElementById('deleteVcNames').value.split(',').map((s) => s.trim()).filter(Boolean);
    const selected = [];
    document.querySelectorAll('#vcList input[type=checkbox]:checked').forEach((cb) => {
      selected.push(cb.dataset.name);
    });
    const names = manual.length ? manual : selected;
    if (!names.length) {
      setErr('Enter comma-separated VC names or select in the list.');
      return;
    }
    try {
      await api('POST', '/api/virtual-clusters/delete', { names });
      msg.textContent = 'Delete requested.';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });

  document.getElementById('btnAlterVc').addEventListener('click', async () => {
    setErr('');
    const msg = document.getElementById('vcMsg');
    const vcName = document.getElementById('alterVcName').value.trim();
    const type = document.getElementById('alterType').value;
    const op = document.getElementById('alterOp').value;
    const name = document.getElementById('alterName').value.trim();
    const physicalTopic = document.getElementById('alterPhysical').value.trim();
    if (!vcName) {
      setErr('Target VC name is required.');
      return;
    }
    if (!name) {
      setErr('Resource name is required.');
      return;
    }
    const change = { type, op, name };
    if (physicalTopic) {
      change.physicalTopic = physicalTopic;
    }
    try {
      await api('POST', '/api/virtual-clusters/alter', { vcName, changes: [change] });
      msg.textContent = 'Alter requested.';
      msg.className = 'ok';
    } catch (e) {
      msg.textContent = '';
      setErr(e.message);
    }
  });
})();
