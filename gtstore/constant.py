'''
Define constants
'''

HEADER_OK = 'OK'

HEADER_NODE_ACTIVE = 'NODE ACTIVE'
HEADER_NODE_TERMINATED = 'NODE TERMINATED'

HEADER_NODE_GET = 'NODE GET'
HEADER_NODE_PUT = 'NODE PUT'

HEADER_NODE_GET_DATABASE = 'NODE GET DATABASE'

HEADER_CLIENT_HELLO = 'CLIENT HELLO'
HEADER_CLIENT_GET = 'CLIENT GET'
HEADER_CLIENT_PUT = 'CLIENT PUT'

HEADER_MASTER_UPDATE_NODE_RING = 'MASTER UPDATE NODE RING'

M = 10
K = 3
TIMEOUT = 1.0 * 10 / K
