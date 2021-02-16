from Crypto.Cipher import AES
from Crypto.Util import Counter
import hashlib

class AESFile:
    """VERY simple wrapper for AES CTR file en/decryption.
    Does the bare minimum, you may get errors if not careful."""
    @staticmethod
    def key(passStr, saltStr, iterations=10000):
        return hashlib.pbkdf2_hmac('sha256', passStr.encode('utf8'),
            saltStr.encode('utf8'), iterations)

    def __init__(self, filename, mode, key):
        ctr = Counter.new(128)
        self.obj = AES.new(key, AES.MODE_CTR, counter=ctr)
        if mode in ('wb', 'rb'): self.fp = open(filename, mode)
        else: raise RuntimeError('Only rb and wb modes supported!')
    def write(self, data): return self.fp.write(self.obj.encrypt(data))
    def read(self, size=-1): return self.obj.decrypt(self.fp.read(size))
    def close(self): self.fp.close()

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
    key = AESFile.key(args.password, args.salt, args.iterations)

    startTime, bs = time.time(), 0

    if args.mode == 'encrypt':
        infile = open(args.input, 'rb')
        outfile = AESFile(args.output, 'wb', key)
    else:
        infile = AESFile(args.input, 'rb', key)
        outfile = open(args.output, 'wb')

    while True:
        data = infile.read(65536)
        if not data: break
        outfile.write(data)
        bs += len(data)

    infile.close()
    outfile.close()

    secs = time.time() - startTime

    print(f'{bs} bytes {args.mode}ed', 'in %.1f seconds, %.2f GB/s' %
            (secs, bs/2**30/secs))
