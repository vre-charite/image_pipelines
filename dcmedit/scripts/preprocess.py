import os
import os.path

def generate(vars, _dir='dicomedit_scripts'):
    des = open(os.path.join(_dir, vars['project']+'.des'), 'r').read()
    expr = os.path.splitext(vars['input_file'])[0].split(os.sep)
    if vars['project'] not in expr: 
        raise ValueError(f"project ID {vars['project']} not in {vars['input_file']}.")
    expr = os.path.join(*expr[expr.index(vars['project']):])
    des = des.replace('project', vars['project'])
    des = des.replace('subject', vars['subject'])
    des = des.replace('session', expr)
    
    vars['anonymize_script'] = os.path.join(vars['ext_dir'], f"{vars['subject']}.des")
    with open(vars['anonymize_script'], 'w') as f:
        f.write(des)
