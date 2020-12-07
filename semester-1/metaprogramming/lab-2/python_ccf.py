import os
from argparse import ArgumentParser

# path = 'D:\\GitHub\\year-4'
# files = [os.path.join(root, f) for root, _, files in os.walk(path) for f in files if f.endswith('.py')]
# print(files)

if __name__ == '__main__':
    parser = ArgumentParser(description='Python code conventions verifier/fixer')

    verify_parser = ArgumentParser(add_help=False)
    mutex_group = verify_parser.add_mutually_exclusive_group(required=True)
    mutex_group.add_argument('-p', help='project path')
    mutex_group.add_argument('-d', help='directory path')
    mutex_group.add_argument('-f', help='file path')

    fix_parser = ArgumentParser(add_help=False)
    mutex_group = fix_parser.add_mutually_exclusive_group(required=True)
    fix_parser.add_argument('-p', help='project path')
    fix_parser.add_argument('-d', help='directory path')
    fix_parser.add_argument('-f', help='file path')

    subparsers = parser.add_subparsers(dest='command')
    verify_subparser = subparsers.add_parser('verify', parents=[verify_parser], help='check if code meets code '
                                                                                     'conventions')
    fix_subparser = subparsers.add_parser('fix', parents=[fix_parser], help='fix code so that it meets code '
                                                                            'conventions')

    args = parser.parse_args()
    if args.command == 'verify':
        if args.p:
            pass
        if args.d:
            pass
        if args.f:
            pass
    if args.command == 'fix':
        if args.p:
            pass
        if args.d:
            pass
        if args.f:
            pass
