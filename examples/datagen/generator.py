import calendar
import json
import logging
import logging.handlers
import random
import socket
import string
import time
import yaml
import ipaddress
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

class DataGenerator:
    """Unified data generator for DNS, firewall, proxy, and ingress-auth events"""

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the generator with config"""
        if config_path:
            self.config = self._load_config(config_path)
        else:
            # Use default config from package
            default_config_path = Path(__file__).parent / 'config.yaml'
            self.config = self._load_config(default_config_path)

    def _load_config(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """Load YAML configuration"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _random_ip_from_cidrs(self, cidr_str: str) -> str:
        """Generate random IP from CIDR range(s)"""
        cidrs = cidr_str.split(',')
        networks = [ipaddress.ip_network(c.strip()) for c in cidrs]
        net = random.choice(networks)
        return str(random.choice(list(net.hosts())))

    def _ip_to_int(self, ip: str) -> int:
        """Convert IP address to integer"""
        octets = ip.split('.')
        return sum(int(octet) << (24 - 8 * i) for i, octet in enumerate(octets))

    def _int_to_ip(self, num: int) -> str:
        """Convert integer to IP address"""
        return '.'.join(str((num >> (24 - 8 * i)) & 255) for i in range(4))

    def _generate_nk_ip(self, ip_ranges: List[List[str]]) -> str:
        """Generate random IP from ranges"""
        ip_range = random.choice(ip_ranges)
        start_ip = self._ip_to_int(ip_range[0])
        end_ip = self._ip_to_int(ip_range[1])
        return self._int_to_ip(random.randint(start_ip, end_ip))

    def _get_timestamp(self, format_type: str) -> str:
        """Generate timestamp in specified format"""
        now = datetime.now()

        if format_type == "%d-%b-%Y %H:%M:%S.%f":
            # DNS format: 25-Sep-2024 14:32:18.123
            return now.strftime("%d-%b-%Y %H:%M:%S.%f")[:-3]
        elif format_type == "%b %d %H:%M:%S":
            # Firewall format: Sep 25 14:32:18
            return now.strftime("%b %d %H:%M:%S")
        elif format_type == "unix_ms":
            # Proxy format: 1727271138.123
            return f"{calendar.timegm(now.timetuple())}.{random.randint(0, 999):03d}"
        elif format_type == "iso8601_ms":
            # Ingress-auth format: 2024-09-25T14:32:18.123Z
            return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        else:
            return now.strftime(format_type)

    def _generate_dynamic_value(self, value_spec: Any) -> Any:
        """Generate dynamic values based on spec"""
        if isinstance(value_spec, str):
            if value_spec == "dynamic_32_hex":
                return ''.join(random.choices(string.hexdigits.lower(), k=32))
            elif value_spec == "dynamic_40_hex":
                return '0x' + ''.join(random.choices(string.hexdigits.lower(), k=40))
            elif value_spec == "dynamic_16_alphanum":
                return 'juche_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))
            elif value_spec.startswith("random_int:"):
                parts = value_spec.split(':')
                min_val, max_val = int(parts[1]), int(parts[2])
                return random.randint(min_val, max_val)
            elif value_spec.startswith("random_hex:"):
                length = int(value_spec.split(':')[1])
                return ''.join(random.choice('0123456789abcdef') for _ in range(length))
        elif isinstance(value_spec, dict) and value_spec.get('type') == 'random_int':
            return random.randint(value_spec['min'], value_spec['max'])

        return value_spec

    def generate_dns_event(self) -> str:
        """Generate DNS event in exact original format"""
        config = self.config['generators']['dns']

        # Get timestamp
        timestamp = self._get_timestamp(config['timestamp_format'])

        # Get random data
        query_type, query_domain = random.choice(config['data']['domains'])
        src_ip = self._random_ip_from_cidrs(config['defaults']['src_cidrs'])
        resolver_ip = self._random_ip_from_cidrs(config['defaults']['src_cidrs'])

        # Generate dynamic fields
        hex_id = self._generate_dynamic_value("random_hex:12")
        src_port = self._generate_dynamic_value("random_int:10000:65535")
        flags = random.choice(config['schema']['fields']['flags'])

        # Build event using original template
        return config['schema']['template'].format(
            timestamp=timestamp,
            hex_id=hex_id,
            src_ip=src_ip,
            src_port=src_port,
            query_domain=query_domain,
            query_type=query_type,
            flags=flags,
            resolver_ip=resolver_ip
        )

    def generate_firewall_event(self) -> str:
        """Generate firewall event in exact original format"""
        config = self.config['generators']['firewall']

        # Get timestamp
        timestamp = self._get_timestamp(config['timestamp_format'])

        # Get random target
        dst_ip, dst_port, proto = random.choice(config['data']['targets'])
        src_ip = self._random_ip_from_cidrs(config['defaults']['src_cidrs'])

        # Generate all fields
        fields = config['schema']['fields']
        rule = self._generate_dynamic_value(fields['rule'])
        sub_rule = fields['sub_rule']  # empty
        anchor = fields['anchor']      # empty
        tracker = self._generate_dynamic_value(fields['tracker'])
        interface = random.choice(fields['interface'])
        reason = fields['reason']
        action = random.choice(fields['action'])
        direction = random.choice(fields['direction'])
        ipver = fields['ipver']
        tos = fields['tos']
        ecn = fields['ecn']
        ttl = self._generate_dynamic_value(fields['ttl'])
        id_ = self._generate_dynamic_value(fields['id'])
        offset = fields['offset']
        flags = random.choice(fields['flags'])
        length = self._generate_dynamic_value(fields['length'])
        src_port = self._generate_dynamic_value("random_int:10000:65535")
        data_len = self._generate_dynamic_value(fields['data_len'])

        # Protocol specific handling
        if proto.lower() == 'tcp':
            proto_id = 6
            proto_text = 'tcp'
            tcp_flags = random.choice(fields['tcp_flags'])
            seq = self._generate_dynamic_value(fields['seq'])
            ack = self._generate_dynamic_value(fields['ack'])
            window = self._generate_dynamic_value(fields['window'])
            urg = fields['urg']
            options = fields['options']
            proto_specific = f"{src_port},{dst_port},{data_len},{tcp_flags},{seq},{ack},{window},{urg},{options}"
        else:  # UDP
            proto_id = 17
            proto_text = 'udp'
            proto_specific = f"{src_port},{dst_port},{data_len}"

        # Build CSV components
        ipv4_data = f"{tos},{ecn},{ttl},{id_},{offset},{flags},{proto_id},{proto_text}"
        ip_data = f"{length},{src_ip},{dst_ip}"
        log_data = f"{rule},{sub_rule},{anchor},{tracker},{interface},{reason},{action},{direction},{ipver},{ipv4_data},{ip_data},{proto_specific}"

        return f"{timestamp} filterlog: {log_data}"

    def generate_proxy_event(self) -> str:
        """Generate proxy event in exact original format"""
        config = self.config['generators']['proxy']

        # Get timestamp
        timestamp = self._get_timestamp(config['timestamp_format'])

        # Get random site
        method, url = random.choice(config['data']['sites'])
        client_ip = self._random_ip_from_cidrs(config['defaults']['src_cidrs'])
        peer = self._random_ip_from_cidrs(config['defaults']['src_cidrs'])

        # Generate fields
        fields = config['schema']['fields']
        elapsed = self._generate_dynamic_value(fields['elapsed'])
        code = random.choice(fields['code'])
        status = random.choice(fields['status'])
        bytes_sent = self._generate_dynamic_value(fields['bytes_sent'])
        user = fields['user']  # "-"
        hierarchy = random.choice(fields['hierarchy'])
        content_type = random.choice(fields['content_type'])

        # Build using original template
        return config['schema']['template'].format(
            timestamp=timestamp,
            elapsed=elapsed,
            client_ip=client_ip,
            code=code,
            status=status,
            bytes_sent=bytes_sent,
            method=method,
            url=url,
            user=user,
            hierarchy=hierarchy,
            peer=peer,
            content_type=content_type
        )

    def generate_ingress_auth_event(self) -> str:
        """Generate ingress-auth event in exact original JSON format"""
        config = self.config['generators']['ingress_auth']
        data = config['data']

        # Build base event
        log_entry = {
            'version': 'v1',
            'event_type': 'INGRESS_AUTHENTICATION',
            'time': self._get_timestamp(config['timestamp_format']),
            'account': random.choice(data['usernames']),
            'account_domain': data['domain'],
            'source_ip': self._generate_nk_ip(data['ip_ranges']),
            'authentication_result': random.choice(data['auth_results']),
            'authentication_target': random.choice(data['auth_targets'])
        }

        # Add custom_data 80% of the time
        if random.random() > 0.2:
            custom_data_item = random.choice(data['custom_data_pool'])
            processed_custom = {}

            for key, value in custom_data_item.items():
                processed_custom[key] = self._generate_dynamic_value(value)

            log_entry['custom_data'] = processed_custom

        return json.dumps(log_entry, ensure_ascii=True)

    def _setup_syslog(self, host: str, port: int, facility: str) -> logging.Logger:
        """Setup syslog logger"""
        logger = logging.getLogger(f'datagen_syslog_{random.randint(1000,9999)}')
        logger.handlers.clear()  # Clear any existing handlers
        logger.setLevel(logging.INFO)

        facility_map = {
            'user': logging.handlers.SysLogHandler.LOG_USER,
            'local0': logging.handlers.SysLogHandler.LOG_LOCAL0
        }

        handler = logging.handlers.SysLogHandler(
            address=(host, port),
            facility=facility_map.get(facility, logging.handlers.SysLogHandler.LOG_USER)
        )
        logger.addHandler(handler)
        return logger

    def _send_via_socket(self, events: List[str], host: str, port: int):
        """Send events via TCP socket"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                for event in events:
                    # Ingress-auth events need newline termination
                    message = (event + '\n').encode('utf-8')
                    s.sendall(message)
        except Exception as e:
            raise Exception(f"Socket connection failed to {host}:{port}: {e}")

    def generate_events(self, generator_type: str, host: Optional[str], port: Optional[int],
                       count: int, interval: float, output: Optional[str]):
        """Main generation method"""

        config = self.config['generators'][generator_type.replace('-', '_')]

        # Determine output method
        if output:
            output_method = output
        elif host:
            output_method = config['output_format']
        else:
            output_method = 'console'

        # Set default ports
        if host and not port:
            if generator_type == 'ingress-auth':
                port = config['defaults']['target_port']
            else:
                port = 514  # default syslog port

        # Generate events
        events = []
        generator_map = {
            'dns': self.generate_dns_event,
            'firewall': self.generate_firewall_event,
            'proxy': self.generate_proxy_event,
            'ingress-auth': self.generate_ingress_auth_event
        }

        generator_func = generator_map[generator_type]

        for i in range(count):
            event = generator_func()
            events.append(event)

            if output_method == 'console':
                print(event)
            elif output_method == 'syslog' and host:
                if i == 0:  # Setup logger once
                    logger = self._setup_syslog(host, port, config.get('syslog_facility', 'user'))
                logger.info(event)

            if interval > 0 and i < count - 1:
                time.sleep(interval)

        # Send via socket if needed (batch for efficiency)
        if output_method == 'socket' and host:
            self._send_via_socket(events, host, port)

        if output_method != 'console':
            print(f"✅ Generated {count} {generator_type} events to {host}:{port}")