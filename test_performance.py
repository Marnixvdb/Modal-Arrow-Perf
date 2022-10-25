import modal

get_data_from_shared_volume = modal.lookup(
    'arrow_receiver', 'get_data_from_shared_volume')
get_data_over_network = modal.lookup('arrow_receiver', 'get_data_over_network')
get_data_as_function_result = modal.lookup(
    'arrow_receiver', 'get_data_as_function_result')

print('get_data_from_shared_volume:')
print(get_data_from_shared_volume())
print('----')
print('get_data_over_network:')
print(get_data_over_network())
print('----')
print('get_data_as_function_result:')
print(get_data_as_function_result())
