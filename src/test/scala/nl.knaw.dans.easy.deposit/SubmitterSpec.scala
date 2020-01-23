/**
 * Copyright (C) 2018 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.deposit

import java.io.IOException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.GroupPrincipal
import java.nio.file.{ NoSuchFileException, Paths }
import java.util.concurrent.Executor

import better.files.{ File, StringExtensions }
import nl.knaw.dans.easy.deposit.Errors.{ CorruptDepositException, InvalidDocumentException }
import nl.knaw.dans.easy.deposit.docs.StateInfo.State
import nl.knaw.dans.easy.deposit.docs.{ DatasetMetadata, StateInfo }
import nl.knaw.dans.easy.deposit.executor.{ JobQueueManager, SystemStatus }
import nl.knaw.dans.lib.error._
import okhttp3.mockwebserver.{ MockResponse, MockWebServer }
import org.apache.commons.configuration.PropertiesConfiguration
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ Assertion, BeforeAndAfterAll }
import scalaj.http.Http

import scala.util.{ Failure, Success, Try }

class SubmitterSpec extends TestSupportFixture with MockFactory with BeforeAndAfterAll {

  override def beforeEach(): Unit = {
    super.beforeEach()
    clearTestDir()
  }

  case class MailExecutor() extends Executor {override def execute(command: Runnable): Unit = () }
  override protected def beforeAll(): Unit = {
    // TODO next line not effective for https://github.com/apache/commons-email/blob/3fd73a641c3f729586824da1c17aa8efd0e895f8/src/main/java/org/apache/commons/mail/Email.java#L637
    //  see also https://github.com/m-szalik/javamail/blob/master/docs/examples/standalone-example/src/main/java/org/jsoftware/javamail/MainAppExample.java
    //  which would write the emails to files but doesn't seem to collaborate with apache.commons.mail
    System.setProperty("mail.event.executor", "nl.knaw.dans.easy.deposit.SubmitterSpec$MailExecutor$")
  }

  // configure the mock server
  private val server = new MockWebServer
  private val agreementTestServer = "/generate/"
  private val agreementGenerator = AgreementGenerator(Http, server.url(agreementTestServer).url(), "text/html")

  override protected def afterAll(): Unit = {
    server.shutdown()
    super.afterAll()
  }

  private val submitDir: File = testDir / "submitted"
  private val stageDir: File = testDir / "staged"
  private val depositHome = "https://easy.dans.knaw.nl/deposit"
  private val easyHome = new URL("https://easy.dans.knaw.nl/ui")
  private val validGroup = principalOf(userGroup)
  private val datasetMetadata: DatasetMetadata = DatasetMetadata(getManualTestResource("datasetmetadata-from-ui-all.json"))
    .getOrRecover(e => fail("could not get test input", e))
    .copy(messageForDataManager = Some("Lorum Ipsum"))

  def init(datasetMetadata: DatasetMetadata = datasetMetadata,
           withDoiInProps: Boolean = false,
          ): (DepositDir, File) = {

    val deposit = DepositDir.create(testDir / "drafts", "user")
      .getOrRecover(e => fail(e.toString, e))

    deposit.writeDatasetMetadataJson(datasetMetadata)

    val bag = deposit.getDataFiles
      .getOrRecover(e => fail(e.toString, e)).bag

    val props = bag.baseDir.parent / "deposit.properties"
    if (withDoiInProps) {
      val doi: String = datasetMetadata.doi
        .getOrElse(fail("could not get DOI from test input"))
      props.append(s"identifier.doi=${ doi }")
    }
    submitDir.createDirectories()
    deposit -> props
  }

  "constructor" should "fail if the configured group does not exist" in {
    val props = minimalAppConfig.properties
    props.setProperty("deposit.permissions.group", "not-existing-group")

    // the App creates the Submitter
    the[IOException] thrownBy new EasyDepositApiApp(new Configuration("", props)) should
      have message "Group not-existing-group could not be found"
  }

  "submit" should "schedule a job" in {
    val (draftDeposit, draftProperties) = init(withDoiInProps = true)

    // pre conditions
    draftProperties.contentAsString should (include("DRAFT") and not include "bag-store.bag-id")

    new Submitter(submitDir, validGroup, depositHome, jobQueue = expectsOneSubmitJob(), mailer = null, agreementGenerator = null)
      .submit(draftDeposit, createStateManager(draftDeposit), defaultUserInfo, stageDir) shouldBe a[Success[_]]

    // post conditions
    draftProperties.contentAsString should (include("SUBMITTED") and include("bag-store.bag-id"))
  }

  it should "catch incomplete metatdata" in {
    val (draftDeposit, _) = init(DatasetMetadata())

    new Submitter(submitDir, validGroup, depositHome, jobQueue = null, mailer = null, agreementGenerator = null)
      .submit(draftDeposit, draftDepositStateManager = null, defaultUserInfo, stageDir) should matchPattern {
      case Failure(e: InvalidDocumentException) if e.getMessage.startsWith("invalid DatasetMetadata: ") =>
    }
  }

  it should "catch missing properties" in {
    val (draftDeposit, _) = init()
    (draftDeposit.bagDir.parent / "deposit.properties").delete()

    new Submitter(submitDir, validGroup, depositHome, jobQueue = null, mailer = null, agreementGenerator = null)
      .submit(draftDeposit, draftDepositStateManager = null, defaultUserInfo, stageDir) should matchPattern {
      case Failure(e: CorruptDepositException) if e.getMessage.contains("deposit.properties not found or empty") =>
    }
  }

  it should "catch a missing bag" in {
    val (draftDeposit, _) = init(withDoiInProps = true)
    draftDeposit.bagDir.delete()

    new Submitter(submitDir, validGroup, depositHome, jobQueue = null, mailer = null, agreementGenerator = null)
      .submit(draftDeposit, draftDepositStateManager = null, defaultUserInfo, stageDir) should matchPattern {
      case Failure(e: NoSuchFileException) if e.getMessage.contains("bag/bagit.txt") =>
    }
  }

  it should "catch inconsistency between actual payloads and manifest" in {
    val (draftDeposit, _) = init(withDoiInProps = true)
    val file = (draftDeposit.bagDir / "data" / "some.file").createFile()

    new Submitter(submitDir, validGroup, depositHome, jobQueue = null, mailer = null, agreementGenerator = null)
      .submit(draftDeposit, createStateManager(draftDeposit), defaultUserInfo, file) should matchPattern {
      case Failure(e: Exception) if e.getMessage ==
        s"invalid bag, missing [files, checksums]: [Set(), Set($file)]" =>
    }
  }

  "SubmitJob.run" should "write all files" in {
    // assemble test data
    val (draftDeposit, draftProperties) = init(withDoiInProps = true)
    val doi = draftDeposit.getDOI(pidRequester = null).map(_.toString).getOrRecover(e => fail("could not get doi from test data", e))
    val bag = draftDeposit.getDataFiles.getOrRecover(e => fail(e.toString, e)).bag
    bag.addPayloadFile("Lorum ipsum".inputStream, Paths.get("folder/text.txt"))
    bag.save()

    // pre conditions
    draftProperties.contentAsString should
      (include("DRAFT") and not include "bag-store.bag-id")
    val jsonFile = draftDeposit.bagDir / "metadata" / "dataset.json"
    val mdOldSize = jsonFile.size // should not change
    stageDir should not(exist)
    submitDir.list shouldBe empty

    server.enqueue { pdfResponse } // agreement
    server.enqueue { okResponse } // mail
    new Submitter(submitDir, validGroup, depositHome, jobQueue = executesSubmitJob(), createMailer, agreementGenerator)
      .submit(draftDeposit, createStateManager(draftDeposit), defaultUserInfo, stageDir) shouldBe Success(())
    expectedAgreementRequest(doi)

    // post conditions
    jsonFile.size shouldBe mdOldSize
    val submittedDeposit = submitDir.children.toSeq.head
    draftProperties.contentAsString should // compare with pre conditions
      (include("SUBMITTED") and include("bag-store.bag-id") and be((submittedDeposit / "deposit.properties").contentAsString))
    val submittedMetadata = submittedDeposit / "bag" / "metadata"
    (draftDeposit.bagDir / "metadata").list.toSeq.map(_.name) shouldBe
      Seq("dataset.json") // compare with submitted metadata files
    submittedMetadata.listRecursively.toSeq.map(_.name) should (have size 5 and contain allOf
      ("depositor-info", "agreements.xml", "message-from-depositor.txt", "dataset.xml", "files.xml"))
    (submittedMetadata / "files.xml").contentAsString should include("""<file filepath="data/folder/text.txt">""")
    (submittedMetadata / "depositor-info" / "message-from-depositor.txt").contentAsString shouldBe
      s"""Lorum Ipsum
         |
         |The deposit can be found at $depositHome/${ draftDeposit.id }""".stripMargin
    submittedDeposit.listRecursively.toSeq should contain allElementsOf Seq(
      "deposit.properties",
      "bag",
      "bag/bagit.txt",
      "bag/bag-info.txt",
      "bag/manifest-sha1.txt",
      "bag/tagmanifest-sha1.txt",
      "bag/data",
      "bag/data/folder",
      "bag/data/folder/text.txt",
      "bag/metadata",
      "bag/metadata/dataset.xml",
      "bag/metadata/files.xml",
      "bag/metadata/depositor-info",
      "bag/metadata/depositor-info/agreements.xml",
      "bag/metadata/depositor-info/message-from-depositor.txt",
    ).map(submittedDeposit / _)
  }

  it should "write empty message-from-depositor file" in {
    // prepare test data
    val (draftDeposit, draftPropertiesFile) = init(
      datasetMetadata.copy(messageForDataManager = None),
      withDoiInProps = true
    )

    // expectations
    server.enqueue { pdfResponse } // agreement
    server.enqueue { okResponse } // mail

    // test
    new Submitter(submitDir, validGroup, depositHome, jobQueue = executesSubmitJob(), createMailer, agreementGenerator)
      .submit(draftDeposit, createStateManager(draftDeposit), defaultUserInfo, stageDir) shouldBe Success(())

    // post condition (other details in previous test)
    val uuid = load(draftPropertiesFile).getString("bag-store.bag-id")
    (submitDir / uuid / "bag" / "metadata" / "depositor-info" / "message-from-depositor.txt").contentAsString shouldBe
      s"The deposit can be found at $depositHome/${ draftDeposit.id }"
  }

  it should "overwrite a previous property bag-store.bag-id at resubmit" in {
    val (draftDeposit, draftPropertiesFile) = init(withDoiInProps = true)
    for {_ <- Seq(1, 2)} {
      server.enqueue { pdfResponse } // agreement
      server.enqueue { okResponse } // mail
    }
    val submitter = new Submitter(submitDir, validGroup, depositHome, jobQueue = executesSubmitJob(times = 2), createMailer, agreementGenerator)

    // not a val as the result changes by steps of the test scenario
    def bagStoreBagId: String = load(draftPropertiesFile).getString("bag-store.bag-id")

    bagStoreBagId shouldBe null // pre condition

    // tested scenario; we need a fresh StateManager for the 2nd submit (as a 2nd service request would provide)

    submitter.submit(draftDeposit, createStateManager(draftDeposit), defaultUserInfo, stageDir) shouldBe Success(())
    draftPropertiesFile // shortcut to mimic reject by ingest-flow or archivist and subsequent save draft
      .write(draftPropertiesFile.contentAsString.replace("SUBMITTED", "DRAFT"))
    val rejectedId = bagStoreBagId // intermediate post condition
    submitter.submit(draftDeposit, createStateManager(draftDeposit), defaultUserInfo, stageDir) shouldBe Success(())
    val resubmittedId = bagStoreBagId // final post condition

    // Q.E.D.
    submitDir.list.toSeq.map(_.name) should (have size 2 and contain allOf(rejectedId, resubmittedId))
  }

  it should "report a group configuration problem" in {
    val (draftDeposit, draftPropertiesFile) = init(withDoiInProps = true)
    val stateManager = createStateManager(draftDeposit)

    new Submitter(submitDir, principalOf(unrelatedGroup), depositHome, jobQueue = executesSubmitJob(), createMailer, agreementGenerator)
      .submit(draftDeposit, stateManager, defaultUserInfo, stageDir) shouldBe Success(())

    // post conditions
    stageDir.list shouldNot be(empty)
    submitDir.list shouldBe empty
    stateInfo(stateManager).stateDescription should fullyMatch regex // TODO change to contact dans message
      "Not able to set the group to .*. Probably the current user (.*) is not part of this group."
    draftPropertiesFile.contentAsString should (include("state.label = SUBMITTED") and include("bag-store.bag-id = "))
  }

  it should "report an unexpected exception" in {
    val (draftDeposit, draftPropertiesFile) = init(withDoiInProps = true)
    val groupPrincipal = new GroupPrincipal() {override def getName: String = "invalidGroupPrincipal" } // causes ProviderMismatchException
    val stateManager = createStateManager(draftDeposit)

    new Submitter(submitDir, groupPrincipal, depositHome, jobQueue = executesSubmitJob(), createMailer, agreementGenerator)
      .submit(draftDeposit, stateManager, defaultUserInfo, stageDir) shouldBe Success(())

    // post conditions
    stageDir.list shouldNot be(empty)
    submitDir.list shouldBe empty
    stateInfo(stateManager) shouldBe StateInfo(State.submitted, s"unexpected error occurred on $stageDir") // TODO change to contact dans message
    draftPropertiesFile.contentAsString should include("bag-store.bag-id = ")
  }

  private def load(file: File) = new PropertiesConfiguration(file.toJava)

  private def principalOf(group: String) = {
    submitDir.fileSystem.getUserPrincipalLookupService.lookupPrincipalByGroupName(group)
  }

  private def stateInfo(stateManager: StateManager) = {
    stateManager.getStateInfo.getOrRecover(e => fail("could not get stateInfo of draft deposit", e))
  }

  private def okResponse = new MockResponse().setResponseCode(200)

  private def pdfResponse = okResponse.addHeader("Content-Type", "application/pdf").setBody("mocked pdf")

  /** use null instead when not expecting to arrive at requesting/changing any state */
  private def createStateManager(draftDeposit: DepositDir) = {
    draftDeposit.getStateManager(submitDir, easyHome)
      .getOrRecover(e => fail(s"could not get stateManager of test deposit $e"))
  }

  private def executesSubmitJob(times: Int = 1): JobQueueManager = {
    val mocked = mock[JobQueueManager]
    (() => mocked.getSystemStatus) expects() repeat times returning
      SystemStatus(threadPoolStatus = null, queueSize = 2, queueContent = null)
    ((mocked.scheduleJob(_: SubmitJob)) expects *).onCall { job: SubmitJob =>
      // TODO sendMimeMessage throws the non-fatal
      //  "EmailException: Sending the email to the following server failed mail.server.does.not.exist.dans.knaw.nl"
      //  note that the required session is set by buildMimeMessage in the Mailer class
      Try(job.run())
    } repeat times
    mocked
  }

  /** use null instead when not expecting to put a job on the queue */
  private def expectsOneSubmitJob(expectedResult: Try[Unit] = Success(())): JobQueueManager = {
    val mocked = mock[JobQueueManager]
    (() => mocked.getSystemStatus) expects() once() returning
      SystemStatus(threadPoolStatus = null, queueSize = 2, queueContent = null)
    (mocked.scheduleJob(_: Runnable)) expects * once() returning expectedResult
    mocked
  }

  /** use null instead when not expecting to compose an email */
  private def createMailer: Mailer = {
    new Mailer(
      smtpHost = "mail.server.does.not.exist.dans.knaw.nl",
      fromAddress = "from.does.not.exist@dans.knaw.nl",
      bounceAddress = "bounce.does.not.exist@dans.knaw.nl",
      bccs = Seq("bcc.does.not.exist@dans.knaw.nl"),
      templateDir = File("src/main/assembly/dist/cfg/template"),
      myDatasets = new URL(depositHome)
    )
  }

  def expectedAgreementRequest(doi: String): Assertion = {
    val request = server.takeRequest()
    request.getRequestLine shouldBe s"POST $agreementTestServer HTTP/1.1"
    request.getBody.readString(StandardCharsets.UTF_8) shouldBe
      s"""{"depositor":{"name":"fullName","address":"","zipcode":"","city":"","country":"","organisation":"","phone":"","email":"does.not.exist@dans.knaw.nl"},"doi":"$doi","title":"title 1","dateSubmitted":"2018-03-22","dateAvailable":"2018-03-14","accessCategory":"OPEN_ACCESS","license":"http://creativecommons.org/publicdomain/zero/1.0","sample":false,"agreementVersion":"4.0","agreementLanguage":"EN"}"""
  }
}
