import modal

get_data_from_shared_volume = modal.lookup(
    'arrow_receiver', 'get_data_from_shared_volume')
get_data_over_network = modal.lookup('arrow_receiver', 'get_data_over_network')


print('get_data_from_shared_volume:')
print(get_data_from_shared_volume())
print('----')
print('get_data_over_network:')
print(get_data_over_network())
