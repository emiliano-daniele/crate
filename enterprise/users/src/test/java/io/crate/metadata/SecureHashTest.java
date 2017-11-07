/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata;

import io.crate.test.integration.CrateUnitTest;
import io.crate.user.SecureHash;
import org.elasticsearch.common.settings.SecureString;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

public class SecureHashTest extends CrateUnitTest {

    private static final SecureString PASSWORD =
        new SecureString("password".toCharArray());

    private static final SecureString INVALID_PASSWORD =
        new SecureString("invalid-password".toCharArray());

    @Test
    public void testHashesWithSameSaltAreEqual() throws Exception {
        SecureRandom rand1 = new SecureRandom();
        rand1.setSeed(1);
        SecureHash hash1 = SecureHash.of(PASSWORD, rand1);
        SecureRandom rand2 = new SecureRandom();
        rand2.setSeed(1);
        SecureHash hash2 = SecureHash.of(PASSWORD, rand2);
        assertEquals(hash1, hash2);
    }

    @Test
    public void testSamePasswordsGenerateDifferentHash() throws Exception {
        SecureRandom rand = new SecureRandom();
        SecureHash hash1 = SecureHash.of(PASSWORD, rand);
        SecureHash hash2 = SecureHash.of(PASSWORD, rand);
        assertNotEquals(hash1, hash2);
    }

    @Test
    public void testVerifyHash() throws Exception {
        SecureHash hash = SecureHash.of(PASSWORD, new SecureRandom());

        assertTrue(hash.verifyHash(PASSWORD));
        assertFalse(hash.verifyHash(INVALID_PASSWORD));
    }

    @Test
    public void testNonAsciiChars() throws InvalidKeySpecException, NoSuchAlgorithmException {
        SecureString pw = new SecureString("πä😉ـص".toCharArray());
        SecureHash hash = SecureHash.of(pw, new SecureRandom());

        assertFalse(hash.verifyHash(INVALID_PASSWORD));
        assertTrue(hash.verifyHash(pw));
    }
}