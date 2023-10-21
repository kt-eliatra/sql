/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SESSION_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;
import org.opensearch.sql.spark.execution.session.CreateSessionRequest;
import org.opensearch.sql.spark.execution.session.Session;
import org.opensearch.sql.spark.execution.session.SessionId;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statement.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.flint.FlintIndexMetadata;
import org.opensearch.sql.spark.flint.FlintIndexMetadataReader;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  private static final Logger LOG = LogManager.getLogger();

  public static final String INDEX_TAG_KEY = "index";
  public static final String DATASOURCE_TAG_KEY = "datasource";
  public static final String SCHEMA_TAG_KEY = "schema";
  public static final String TABLE_TAG_KEY = "table";
  public static final String CLUSTER_NAME_TAG_KEY = "cluster";

  private EMRServerlessClient emrServerlessClient;

  private DataSourceService dataSourceService;

  private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  private JobExecutionResponseReader jobExecutionResponseReader;

  private FlintIndexMetadataReader flintIndexMetadataReader;

  private Client client;

  private SessionManager sessionManager;

  public DispatchQueryResponse dispatch(DispatchQueryRequest dispatchQueryRequest) {
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())) {
      return handleSQLQuery(dispatchQueryRequest);
    } else {
      // Since we don't need any extra handling for PPL, we are treating it as normal dispatch
      // Query.
      return handleNonIndexQuery(dispatchQueryRequest);
    }
  }

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    // todo. refactor query process logic in plugin.
    if (asyncQueryJobMetadata.isDropIndexQuery()) {
      return DropIndexResult.fromJobId(asyncQueryJobMetadata.getJobId()).result();
    }

    JSONObject result;
    if (asyncQueryJobMetadata.getSessionId() == null) {
      // either empty json when the result is not available or data with status
      // Fetch from Result Index
      result =
          jobExecutionResponseReader.getResultFromOpensearchIndex(
              asyncQueryJobMetadata.getJobId(), asyncQueryJobMetadata.getResultIndex());
    } else {
      // when session enabled, jobId in asyncQueryJobMetadata is actually queryId.
      result =
          jobExecutionResponseReader.getResultWithQueryId(
              asyncQueryJobMetadata.getJobId(), asyncQueryJobMetadata.getResultIndex());
    }
    // if result index document has a status, we are gonna use the status directly; otherwise, we
    // will use emr-s job status.
    // That a job is successful does not mean there is no error in execution. For example, even if
    // result
    // index mapping is incorrect, we still write query result and let the job finish.
    // That a job is running does not mean the status is running. For example, index/streaming Query
    // is a
    // long-running job which runs forever. But we need to return success from the result index
    // immediately.
    if (result.has(DATA_FIELD)) {
      JSONObject items = result.getJSONObject(DATA_FIELD);

      // If items have STATUS_FIELD, use it; otherwise, mark failed
      String status = items.optString(STATUS_FIELD, JobRunState.FAILED.toString());
      result.put(STATUS_FIELD, status);

      // If items have ERROR_FIELD, use it; otherwise, set empty string
      String error = items.optString(ERROR_FIELD, "");
      result.put(ERROR_FIELD, error);
    } else {
      if (asyncQueryJobMetadata.getSessionId() != null) {
        SessionId sessionId = new SessionId(asyncQueryJobMetadata.getSessionId());
        Optional<Session> session = sessionManager.getSession(sessionId);
        if (session.isPresent()) {
          // todo, statementId == jobId if statement running in session.
          StatementId statementId = new StatementId(asyncQueryJobMetadata.getJobId());
          Optional<Statement> statement = session.get().get(statementId);
          if (statement.isPresent()) {
            StatementState statementState = statement.get().getStatementState();
            result.put(STATUS_FIELD, statementState.getState());
            result.put(ERROR_FIELD, "");
          } else {
            throw new IllegalArgumentException("no statement found. " + statementId);
          }
        } else {
          throw new IllegalArgumentException("no session found. " + sessionId);
        }
      } else {
        // make call to EMR Serverless when related result index documents are not available
        GetJobRunResult getJobRunResult =
            emrServerlessClient.getJobRunResult(
                asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId());
        String jobState = getJobRunResult.getJobRun().getState();
        result.put(STATUS_FIELD, jobState);
        result.put(ERROR_FIELD, "");
      }
    }

    return result;
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    if (asyncQueryJobMetadata.getSessionId() != null) {
      SessionId sessionId = new SessionId(asyncQueryJobMetadata.getSessionId());
      Optional<Session> session = sessionManager.getSession(sessionId);
      if (session.isPresent()) {
        // todo, statementId == jobId if statement running in session.
        StatementId statementId = new StatementId(asyncQueryJobMetadata.getJobId());
        Optional<Statement> statement = session.get().get(statementId);
        if (statement.isPresent()) {
          statement.get().cancel();
          return statementId.getId();
        } else {
          throw new IllegalArgumentException("no statement found. " + statementId);
        }
      } else {
        throw new IllegalArgumentException("no session found. " + sessionId);
      }
    } else {
      CancelJobRunResult cancelJobRunResult =
          emrServerlessClient.cancelJobRun(
              asyncQueryJobMetadata.getApplicationId(), asyncQueryJobMetadata.getJobId());
      return cancelJobRunResult.getJobRunId();
    }
  }

  private DispatchQueryResponse handleSQLQuery(DispatchQueryRequest dispatchQueryRequest) {
    if (SQLQueryUtils.isIndexQuery(dispatchQueryRequest.getQuery())) {
      IndexDetails indexDetails =
          SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
      if (indexDetails.isDropIndex()) {
        return handleDropIndexQuery(dispatchQueryRequest, indexDetails);
      } else {
        return handleIndexQuery(dispatchQueryRequest, indexDetails);
      }
    } else {
      return handleNonIndexQuery(dispatchQueryRequest);
    }
  }

  private DispatchQueryResponse handleIndexQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexDetails indexDetails) {
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);
    tags.put(INDEX_TAG_KEY, indexDetails.getIndexName());
    tags.put(TABLE_TAG_KEY, fullyQualifiedTableName.getTableName());
    tags.put(SCHEMA_TAG_KEY, fullyQualifiedTableName.getSchemaName());
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(
                    dataSourceService.getRawDataSourceMetadata(
                        dispatchQueryRequest.getDatasource()))
                .structuredStreaming(indexDetails.getAutoRefresh())
                .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                .build()
                .toString(),
            tags,
            indexDetails.getAutoRefresh(),
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    return new DispatchQueryResponse(jobId, false, dataSourceMetadata.getResultIndex(), null);
  }

  private DispatchQueryResponse handleNonIndexQuery(DispatchQueryRequest dispatchQueryRequest) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "non-index-query";
    Map<String, String> tags = getDefaultTagsForJobSubmission(dispatchQueryRequest);

    if (sessionManager.isEnabled()) {
      Session session;
      if (dispatchQueryRequest.getSessionId() != null) {
        // get session from request
        SessionId sessionId = new SessionId(dispatchQueryRequest.getSessionId());
        Optional<Session> createdSession = sessionManager.getSession(sessionId);
        if (createdSession.isEmpty()) {
          throw new IllegalArgumentException("no session found. " + sessionId);
        }
        session = createdSession.get();
      } else {
        // create session if not exist
        session =
            sessionManager.createSession(
                new CreateSessionRequest(
                    jobName,
                    dispatchQueryRequest.getApplicationId(),
                    dispatchQueryRequest.getExecutionRoleARN(),
                    SparkSubmitParameters.Builder.builder()
                        .className(FLINT_SESSION_CLASS_NAME)
                        .dataSource(
                            dataSourceService.getRawDataSourceMetadata(
                                dispatchQueryRequest.getDatasource()))
                        .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams()),
                    tags,
                    dataSourceMetadata.getResultIndex(),
                    dataSourceMetadata.getName()));
      }
      StatementId statementId =
          session.submit(
              new QueryRequest(
                  dispatchQueryRequest.getLangType(), dispatchQueryRequest.getQuery()));
      return new DispatchQueryResponse(
          statementId.getId(),
          false,
          dataSourceMetadata.getResultIndex(),
          session.getSessionId().getSessionId());
    } else {
      StartJobRequest startJobRequest =
          new StartJobRequest(
              dispatchQueryRequest.getQuery(),
              jobName,
              dispatchQueryRequest.getApplicationId(),
              dispatchQueryRequest.getExecutionRoleARN(),
              SparkSubmitParameters.Builder.builder()
                  .dataSource(
                      dataSourceService.getRawDataSourceMetadata(
                          dispatchQueryRequest.getDatasource()))
                  .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                  .build()
                  .toString(),
              tags,
              false,
              dataSourceMetadata.getResultIndex());
      String jobId = emrServerlessClient.startJobRun(startJobRequest);
      return new DispatchQueryResponse(jobId, false, dataSourceMetadata.getResultIndex(), null);
    }
  }

  private DispatchQueryResponse handleDropIndexQuery(
      DispatchQueryRequest dispatchQueryRequest, IndexDetails indexDetails) {
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.getRawDataSourceMetadata(dispatchQueryRequest.getDatasource());
    dataSourceUserAuthorizationHelper.authorizeDataSource(dataSourceMetadata);
    FlintIndexMetadata indexMetadata = flintIndexMetadataReader.getFlintIndexMetadata(indexDetails);
    // if index is created without auto refresh. there is no job to cancel.
    String status = JobRunState.FAILED.toString();
    try {
      if (indexMetadata.isAutoRefresh()) {
        emrServerlessClient.cancelJobRun(
            dispatchQueryRequest.getApplicationId(), indexMetadata.getJobId());
      }
    } finally {
      String indexName = indexDetails.openSearchIndexName();
      try {
        AcknowledgedResponse response =
            client.admin().indices().delete(new DeleteIndexRequest().indices(indexName)).get();
        if (!response.isAcknowledged()) {
          LOG.error("failed to delete index");
        }
        status = JobRunState.SUCCESS.toString();
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("failed to delete index");
      }
    }
    return new DispatchQueryResponse(
        new DropIndexResult(status).toJobId(), true, dataSourceMetadata.getResultIndex(), null);
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }

  @Getter
  @RequiredArgsConstructor
  public static class DropIndexResult {
    private static final int PREFIX_LEN = 10;

    private final String status;

    public static DropIndexResult fromJobId(String jobId) {
      String status = new String(Base64.getDecoder().decode(jobId)).substring(PREFIX_LEN);
      return new DropIndexResult(status);
    }

    public String toJobId() {
      String queryId = RandomStringUtils.randomAlphanumeric(PREFIX_LEN) + status;
      return Base64.getEncoder().encodeToString(queryId.getBytes(StandardCharsets.UTF_8));
    }

    public JSONObject result() {
      JSONObject result = new JSONObject();
      if (JobRunState.SUCCESS.toString().equalsIgnoreCase(status)) {
        result.put(STATUS_FIELD, status);
        // todo. refactor response handling.
        JSONObject dummyData = new JSONObject();
        dummyData.put("result", new JSONArray());
        dummyData.put("schema", new JSONArray());
        dummyData.put("applicationId", "fakeDropIndexApplicationId");
        result.put(DATA_FIELD, dummyData);
      } else {
        result.put(STATUS_FIELD, status);
        result.put(ERROR_FIELD, "failed to drop index");
      }
      return result;
    }
  }
}
