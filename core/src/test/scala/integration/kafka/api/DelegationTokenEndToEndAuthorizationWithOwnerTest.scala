/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.api

import kafka.security.JaasTestUtils
import kafka.utils._
import org.apache.kafka.clients.admin.{Admin, CreateDelegationTokenOptions, DescribeDelegationTokenOptions}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourceType.USER
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.token.delegation.DelegationToken
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.util.Using

class DelegationTokenEndToEndAuthorizationWithOwnerTest extends DelegationTokenEndToEndAuthorizationTest {

  def AclTokenCreate = new AclBinding(new ResourcePattern(USER, clientPrincipal.toString, LITERAL),
    new AccessControlEntry(tokenRequesterPrincipal.toString, "*", AclOperation.CREATE_TOKENS, AclPermissionType.ALLOW))
  def TokenCreateAcl = Set(new AccessControlEntry(tokenRequesterPrincipal.toString, "*", AclOperation.CREATE_TOKENS, AclPermissionType.ALLOW))

  // tests the naive positive case for token requesting for others
  def AclTokenDescribe = new AclBinding(new ResourcePattern(USER, clientPrincipal.toString, LITERAL),
    new AccessControlEntry(tokenRequesterPrincipal.toString, "*", AclOperation.DESCRIBE_TOKENS, AclPermissionType.ALLOW))
  def TokenDescribeAcl = Set(new AccessControlEntry(tokenRequesterPrincipal.toString, "*", AclOperation.DESCRIBE_TOKENS, AclPermissionType.ALLOW))

  // This permission is just there so that otherClientPrincipal shows up among the resources
  def AclTokenOtherDescribe = new AclBinding(new ResourcePattern(USER, otherClientPrincipal.toString, LITERAL),
    new AccessControlEntry(otherClientRequesterPrincipal.toString, "*", AclOperation.DESCRIBE_TOKENS, AclPermissionType.ALLOW))


  override def createDelegationTokenOptions(): CreateDelegationTokenOptions = new CreateDelegationTokenOptions().owner(clientPrincipal)

  private val tokenRequesterPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KAFKA_SCRAM_USER_2)
  private val tokenRequesterPassword = JaasTestUtils.KAFKA_SCRAM_PASSWORD_2

  private val otherClientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "other-client-principal")
  private val otherClientPassword = "other-client-password"

  private val otherClientRequesterPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "other-client-requester-principal")
  private val otherClientRequesterPassword = "other-client-requester-password"

  private val describeTokenFailPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "describe-token-fail-principal")
  private val describeTokenFailPassword = "describe-token-fail-password"

  override def configureSecurityAfterServersStart(): Unit = {
    // Create the Acls before calling super which will create the additional tokens
    Using.resource(createPrivilegedAdminClient()) { superuserAdminClient =>
      superuserAdminClient.createAcls(List(AclTokenOtherDescribe, AclTokenCreate, AclTokenDescribe).asJava).values

      brokers.foreach { s =>
        TestUtils.waitAndVerifyAcls(TokenCreateAcl ++ TokenDescribeAcl, s.dataPlaneRequestProcessor.authorizerPlugin.get,
          new ResourcePattern(USER, clientPrincipal.toString, LITERAL))
      }
    }

    super.configureSecurityAfterServersStart()
  }

  override def createAdditionalCredentialsAfterServersStarted(): Unit = {
    createScramCredentialsViaPrivilegedAdminClient(tokenRequesterPrincipal.getName, tokenRequesterPassword)
    createScramCredentialsViaPrivilegedAdminClient(otherClientPrincipal.getName, otherClientPassword)
    createScramCredentialsViaPrivilegedAdminClient(otherClientRequesterPrincipal.getName, otherClientRequesterPassword)
    createScramCredentialsViaPrivilegedAdminClient(describeTokenFailPrincipal.getName, describeTokenFailPassword)
  }

  override def assertToken(token: DelegationToken): Unit = {
    assertTokenOwner(clientPrincipal, token)
    assertTokenRequester(tokenRequesterPrincipal, token)
  }

  override def createTokenRequesterAdminClient(): Admin = {
    createScramAdminClient(kafkaClientSaslMechanism, tokenRequesterPrincipal.getName, tokenRequesterPassword)
  }

  @Test
  def testCreateTokenForOtherUserFails(): Unit = {
    val thrown = assertThrows(classOf[ExecutionException], () => {
      createDelegationTokens(() => new CreateDelegationTokenOptions().owner(otherClientPrincipal), assert = false)
    })
    assertTrue(thrown.getMessage.contains("Delegation Token authorization failed"))
  }

  @Test
  def testDescribeTokenForOtherUserFails(): Unit = {
    Using.resource(createScramAdminClient(kafkaClientSaslMechanism, describeTokenFailPrincipal.getName, describeTokenFailPassword)) { describeTokenFailAdminClient =>
      Using.resource(createScramAdminClient(kafkaClientSaslMechanism, otherClientPrincipal.getName, otherClientPassword)) { otherClientAdminClient =>
        otherClientAdminClient.createDelegationToken().delegationToken().get()
        val tokens = describeTokenFailAdminClient.describeDelegationToken(
          new DescribeDelegationTokenOptions().owners(util.List.of(otherClientPrincipal))
        ).delegationTokens.get.asScala
        assertTrue(tokens.isEmpty)
      }
    }
  }

  @Test
  def testDescribeTokenForOtherUserPasses(): Unit = {
    val adminClient = createTokenRequesterAdminClient()
    try {
      val tokens = adminClient.describeDelegationToken(
        new DescribeDelegationTokenOptions().owners(util.List.of(clientPrincipal)))
        .delegationTokens.get.asScala
      assertTrue(tokens.nonEmpty)
      tokens.foreach(t => {
        assertTrue(t.tokenInfo.owner.equals(clientPrincipal))
        assertTrue(t.tokenInfo.tokenRequester.equals(tokenRequesterPrincipal))
      })
    } finally {
      adminClient.close()
    }
  }
}
