import hashlib
from Crypto.Cipher import AES
from Crypto.Util import Counter
from Crypto.Random import get_random_bytes

def encrypt(infile, outfile, key):
    ctr = Counter.new(128)
    obj = AES.new(key, AES.MODE_CTR, counter=ctr)
    bs = 0

    with open(infile, 'rb') as fin:
        with open(outfile, 'wb') as fout:
            while True:
                data = fin.read(65536)
                if not data: break
                fout.write(obj.encrypt(data))
                bs += len(data)

    return bs

def decrypt(infile, outfile, key):
    ctr = Counter.new(128)
    obj = AES.new(key, AES.MODE_CTR, counter=ctr)
    bs = 0

    with open(infile, 'rb') as fin:
        with open(outfile, 'wb') as fout:
            while True:
                data = fin.read(65536)
                if not data: break
                fout.write(obj.decrypt(data))
                bs += len(data)

    return bs

if __name__ == "__main__":
    import argparse, time

    parser = argparse.ArgumentParser(description='Encrypt/decrypt files')
    parser.add_argument('mode', type=str, choices=['encrypt','decrypt'], help='Mode')
    parser.add_argument('input', type=str, help='Input file')
    parser.add_argument('output', type=str, help='Output file')
    parser.add_argument('password', type=str, help='Password')
    parser.add_argument('salt', type=str, help='Salt')
    parser.add_argument('-i', '--iterations', type=int, default=100000,
            help='PBKDF2 iterations (default 100000)')
    args = parser.parse_args()
    

    key = hashlib.pbkdf2_hmac('sha256', args.password.encode('utf8'),
            args.salt.encode('utf8'), args.iterations)

    startTime = time.time()
    bs = (encrypt if args.mode == 'encrypt' else decrypt) \
            (args.input, args.output, key)
    secs = time.time() - startTime

    print(f'{bs} bytes {args.mode}ed', 'in %.1f seconds, %.2f GB/s' %
            (secs, bs/2**30/secs))
