from ring_node import RingNode

node = RingNode('config_teste.txt', 9999)
print("next_ip:     ", node.next_ip)
print("next_port:   ", node.next_port)
print("nickname:    ", node.nickname)
print("token_time:  ", node.token_time)
print("is_creator:  ", node.is_token_creator)
print("data_time:   ", node.data_time)
