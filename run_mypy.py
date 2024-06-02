from mypy.api import run

if __name__ == '__main__':
    run(['main.py',  '--show-traceback', '--no-incremental'])
