import os
import subprocess
import argparse
import yaml

def compile_kb(config_dict, compile_script_path, hook_script_path, path_prefix):
    for kb in config_dict:
        path = config_dict[kb]['path']
        path = os.path.join(path_prefix, path)
        nodes = config_dict[kb]['nodes']
        edges = config_dict[kb]['edges']
        command_list = ['swipl', '-q', '-s', compile_script_path, '--', '-p', path, '-h', hook_script_path]
        if nodes: 
            # Make sure path/nodes.pl exists
            if not os.path.exists(os.path.join(path, 'nodes.pl')):
                print(f'Error: {os.path.join(path, "nodes.pl")} does not exist!')
                continue
            command_list.append('-n')
        if edges:
            # Make sure path/edges.pl exists
            if not os.path.exists(os.path.join(path, 'edges.pl')):
                print(f'Error: {os.path.join(path, "edges.pl")} does not exist!')
                continue
            command_list.append('-e')
        
        print(f'Compiling {kb}...')
        #execute the command
        subprocess.run(command_list)
        print(f'{kb} compiled successfully!')
    
    print('All KBs compiled successfully!')

def parse_args():
    parser = argparse.ArgumentParser(description='Compile Knowledge Bases')
    parser.add_argument('--config-path', type=str, help='Path to the config file')
    parser.add_argument('--compile-script', type=str, help='Path to the script that compiles the KBs')
    parser.add_argument('--path-prefix', type=str, help='Prefix to add to the path of the KBs')
    parser.add_argument('--hook-script', type=str, help='Path to the hooks file')
    return parser.parse_args()

def main():
    args = parse_args()
    with open(args.config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    if not args.path_prefix:
        print(f'Error: path_prefix is required!')
        return
    
    #make sure the compile script exists
    if not os.path.exists(args.compile_script):
        print(f'Error: {args.compile_script} does not exist!')
        return
    
    if not args.hook_script:
        print(f'Error: hook_path is required!')
        return
    
    compile_kb(config_dict, args.compile_script, args.hook_script ,args.path_prefix)
    

if __name__ == '__main__':
    main()
    