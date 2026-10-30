import os
import subprocess
import argparse
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

def _compile_kb(kb, kb_config, compile_script_path, hook_script_path, path_prefix):
    path = os.path.join(path_prefix, kb_config["path"])
    nodes = kb_config["nodes"]
    edges = kb_config["edges"]
    command_list = ['swipl', '-q', '-s', compile_script_path, '--', '-p', path, '-h', hook_script_path]
    if nodes: 
        # Make sure path/nodes.pl exists
        if not os.path.exists(os.path.join(path, 'nodes.pl')):
           return kb, False, f'{os.path.join(path, "nodes.pl")} does not exist'
        command_list.append('-n')
    if edges:
        # Make sure path/edges.pl exists
        if not os.path.exists(os.path.join(path, 'edges.pl')):
            return kb, False, f'{os.path.join(path, "edges.pl")} does not exist'
        command_list.append('-e')

    logger.info(f'Compiling {kb}...')
    subprocess.run(command_list)
    return kb, True, None

def compile_kbs(config_dict, compile_script_path, 
                hook_script_path, path_prefix, workers=None):
    futures = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for kb, kb_config in config_dict.items():
            f = executor.submit(_compile_kb, kb, kb_config, 
                                     compile_script_path, hook_script_path, path_prefix)
            futures[f] = kb
    
    all_ok = True
    for future in as_completed(futures):
        kb, ok, err = future.result()
        if ok:
            logger.success(f'{kb} compiled successfully!')
        else:
            logger.error(f'{kb}: {err}')
            all_ok = False
    
    if all_ok:
        logger.success('All KBs compiled successfully!')

def parse_args():
    parser = argparse.ArgumentParser(description='Compile Knowledge Bases')
    parser.add_argument('--config-path', type=str, help='Path to the config file')
    parser.add_argument('--compile-script', type=str, help='Path to the script that compiles the KBs')
    parser.add_argument('--hook-script', type=str, help='Path to the hooks file')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of parallel workers (default: number of CPUs)')
    return parser.parse_args()

def main(): 
    args = parse_args()
    with open(args.config_path, 'r') as f:
        config = yaml.safe_load(f)
        config_dict = {k: v for k, v in config['kbs'].items() if v.get('loaded', False)}
        path_prefix = config['settings']['prolog_base']
    
    #make sure the compile script exists
    if not os.path.exists(args.compile_script):
        logger.error(f'Error: {args.compile_script} does not exist!')
        return
    
    if not args.hook_script:
        logger.error('Error: hook_path is required!')
        return
    
    compile_kbs(config_dict, args.compile_script, args.hook_script ,path_prefix, args.workers)
    

if __name__ == '__main__':
    main()
    
