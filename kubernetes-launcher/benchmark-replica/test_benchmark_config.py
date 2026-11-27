import copy
import json
from pathlib import Path
import unittest

from benchmark_config import manifest, validate


class NetworkLimitTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(Path(__file__).with_name('experiment_sample.json').read_text())

    def test_limits_apply_to_every_pod_without_privileging_replica(self):
        self.config['network_interface'] = 'net1'
        self.config['network_limit'].update(rate_kbit_s=2500, burst_bytes=65536, latency_ms=50)
        config = validate(self.config)
        resources = manifest(config)['items']
        pods = [item for item in resources if item['kind'] == 'Pod']
        self.assertEqual(len(pods), config['n_replicas'])
        for pod in pods:
            spec = pod['spec']
            init, = spec['initContainers']
            self.assertEqual(init['image'], config['network_limit']['tools_image'])
            self.assertEqual(init['command'], ['tc'])
            self.assertEqual(init['args'], [
                'qdisc', 'replace', 'dev', 'net1', 'root', 'tbf',
                'rate', '2500kbit', 'burst', '65536', 'latency', '50ms'])
            self.assertEqual(init['securityContext']['capabilities']['add'], ['NET_ADMIN'])
            self.assertFalse(init['securityContext']['runAsNonRoot'])
            replica, = spec['containers']
            self.assertEqual(replica['securityContext']['capabilities'], {'drop': ['ALL']})
            self.assertTrue(spec['securityContext']['runAsNonRoot'])
            self.assertEqual([p['containerPort'] for p in replica['ports']], [19001, 19002, 8081])
        for item in resources:
            if item['kind'] == 'ConfigMap':
                self.assertNotIn('network_limit', json.loads(item['data']['replica.json']))

    def test_old_and_disabled_configs_need_no_init_container(self):
        for remove in (True, False):
            config = copy.deepcopy(self.config)
            if remove:
                del config['network_limit']
            else:
                config['network_limit'] = None
            validated = validate(config)
            self.assertIsNone(validated['network_limit'])
            for item in manifest(validated)['items']:
                if item['kind'] == 'Pod':
                    self.assertNotIn('initContainers', item['spec'])

    def test_rejects_invalid_limits(self):
        for key in ('rate_kbit_s', 'burst_bytes', 'latency_ms'):
            for value in (0, -1, True, 1.5, '10mbit', None):
                with self.subTest(key=key, value=value):
                    config = copy.deepcopy(self.config)
                    config['network_limit'][key] = value
                    with self.assertRaises(ValueError):
                        validate(config)
        for value in ({}, [], {'rate_kbit_s': 10000}, {**self.config['network_limit'], 'typo': 1},
                      {**self.config['network_limit'], 'tools_image': ''}):
            with self.subTest(value=value):
                config = {**self.config, 'network_limit': value}
                with self.assertRaises(ValueError):
                    validate(config)


if __name__ == '__main__':
    unittest.main()
