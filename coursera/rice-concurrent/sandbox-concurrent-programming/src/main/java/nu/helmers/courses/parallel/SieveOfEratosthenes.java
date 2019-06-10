package nu.helmers.courses.parallel;

import java.util.stream.LongStream;

/**
 * In week 3 of the course "Concurrent Programming in Java" (Coursera) Prof. Sarkar illustrated how you can use a
 * dynamically created pipeline of 'actors' to implement a Sieve of Eratosthenes.
 * <p/>
 * I was wondering why you would want to use 'actors' for this algorithm, tried to implement it using plain Java code
 * (no special concurrency constructs, no actors or whatever) and ended up with this.
 * <p/>
 * But in the end it turned out the proposed Java implementation of Prof. Sarkar is very close to what I've written:
 * If you rename Sieve.filter(long n) to Sieve.process(Object message), nextSieve to nextActor, etc. my solution can
 * also be viewed as an actor. The 'mailbox' is not visible here, but the actor does respond to messages.
 */
public class SieveOfEratosthenes {

    private final static long UPPER_BOUND = 200_000;

    public static void main(String[] args) {
        Sieve sieve_2 = new Sieve(2);
        LongStream.rangeClosed(3, UPPER_BOUND).forEach(l -> sieve_2.filter(l));
    }

    private static class Sieve {
        private final long prime;
        private Sieve nextSieve;

        public Sieve(long n) {
            prime = n;
            System.out.println(prime);
        }

        private void filter(long m) {
            if (m % prime == 0) {
                return; // Not a prime. Drop it.
            }
            if (nextSieve == null) {
                nextSieve = new Sieve(m);
            } else {
                nextSieve.filter(m);
            }
        }
    }
}