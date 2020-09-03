import os
import os.path

def generate(vars, _dir='dicomedit_scripts'):
    des = open(os.path.join(_dir, vars['project']+'.des'), 'r').read()
    expr = os.path.splitext(vars['input_file'])[0].split(os.sep)
    if vars['project'] not in expr: 
        raise ValueError(f"project ID {vars['project']} not in {vars['input_file']}.")
    expr = os.path.join(*expr[expr.index(vars['project']):])
    des = des.replace('project', str(vars['project'].encode('utf-8'))[2:-1])
    des = des.replace('subject', str(vars['subject'].encode('utf-8'))[2:-1])
    des = des.replace('session', str(expr.encode('utf-8'))[2:-1])
    
    vars['anonymize_script'] = os.path.join(vars['ext_dir'], f"{vars['subject']}.des")
    with open(vars['anonymize_script'], 'w') as f:
        f.write(str(des))
