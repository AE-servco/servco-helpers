

def reformat_supabase_dict(data):
    output = {}
    for k, v in data.items():
        if v['state']:
            output[v['state']] = v
            if 'id' in output[v['state']]:
                del output[v['state']]['id']
            del output[v['state']]['state']
    
    return output