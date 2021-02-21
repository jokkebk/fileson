#!/usr/bin/env python3
from Crypto.Cipher import AES
from Crypto.Util import Counter
import hashlib, os

class AESFile:
    """On-the-fly AES encryption (on read) and decryption (on write).
    When reading, returns 16 bytes of iv first, then encrypted payload.
    On writing, first 16 bytes are assumed to contain the iv.
    Does the bare minimum, you may get errors if not careful."""
    @staticmethod
    def key(passStr, saltStr, iterations=100000):
        return hashlib.pbkdf2_hmac('sha256', passStr.encode('utf8'),
            saltStr.encode('utf8'), iterations)

    def initAES(self):
        self.obj = AES.new(self.key, AES.MODE_CTR, counter=Counter.new(
            128, initial_value=int.from_bytes(self.iv, byteorder='big')))

    def __init__(self, filename, mode, key, iv=None):
        if not mode in ('wb', 'rb'): 
            raise RuntimeError('Only rb and wb modes supported!')

        self.pos = 0
        self.key = key
        self.mode = mode
        self.fp = open(filename, mode)

        if mode == 'rb':
            self.iv = iv or os.urandom(16)
            self.initAES()
        else: self.iv = bytearray(16)

    def write(self, data):
        datalen = len(data)
        if self.pos < 16:
            ivlen = min(16-self.pos, datalen)
            self.iv[self.pos:self.pos+ivlen] = data[:ivlen]
            self.pos += ivlen
            if self.pos == 16: self.initAES() # ready to init now
            data = data[ivlen:]
        if data: self.pos += self.fp.write(self.obj.decrypt(data))
        return datalen

    def read(self, size=-1):
        ivpart = b''
        if self.pos < 16:
            if size == -1: ivpart = self.iv
            else:
                ivpart = self.iv[self.pos:min(16, self.pos+size)]
                size -= len(ivpart)
        enpart = self.obj.encrypt(self.fp.read(size)) if size else b''
        self.pos += len(ivpart) + len(enpart)
        return ivpart + enpart

    def tell(self): return self.pos

    # only in read mode (encrypting)
    def seek(self, offset, whence=0): # enough seek to satisfy AWS boto3
        if offset: raise RuntimeError('Only seek(0, whence) supported')

        self.fp.seek(offset, whence) # offset=0 works for all whences
        if whence==0: # absolute positioning, offset=0
            self.pos = 0
            self.initAES()
        elif whence==2: # relative to file end, offset=0
            self.pos = 16 + self.fp.tell()

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
        infile = AESFile(args.input, 'rb', key)
        outfile = open(args.output, 'wb')
    else:
        infile = open(args.input, 'rb')
        outfile = AESFile(args.output, 'wb', key)

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
