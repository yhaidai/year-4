import random

import numpy as np


def primes_from_2_to(n):
    """ Input n>=6, Returns a array of primes, 2 <= p < n """
    sieve = np.ones(n // 3 + (n % 6 == 2), dtype=bool)
    for i in range(1, int(n ** 0.5) // 3 + 1):
        if sieve[i]:
            k = 3 * i + 1 | 1
            sieve[k * k // 3::2 * k] = False
            sieve[k * (k - 2 * (i & 1) + 4) // 3::2 * k] = False
    return np.r_[2, 3, ((3 * np.nonzero(sieve)[0][1:] + 1) | 1)]


def miller_rabin(n, k):
    if n == 2:
        return True

    if n % 2 == 0:
        return False

    r, s = 0, n - 1
    while s % 2 == 0:
        r += 1
        s //= 2
    for _ in range(k):
        a = random.randrange(2, n - 1)
        x = pow(a, s, n)
        if x == 1 or x == n - 1:
            continue
        for _ in range(r - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def big_prime(s, primes):
    for r in range(s, 4 * s + 3):
        n = s * r + 1
        for p in primes:
            if n % p == 0:
                break
        else:
            if miller_rabin(n, 40):
                return n


if __name__ == '__main__':
    primes = primes_from_2_to(140_000)

    p = int(primes[-1])
    for _ in range(3):
        p = big_prime(p, primes)
        print('BIG PRIME:', p, len(str(p)))
