/**
 * The MIT License
 * Copyright © 2010 JmxTrans team
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.googlecode.jmxtrans.model.output.elastic;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.googlecode.jmxtrans.exceptions.LifecycleException;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;
import com.googlecode.jmxtrans.model.ValidationException;
import com.googlecode.jmxtrans.model.output.BaseOutputWriter;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.googlecode.jmxtrans.util.NumberUtils.isNumeric;

/**
 * Feed data directly into elastic.
 *
 * @author Peter Paul Bakker - pp@stokpop.nl
 */

@NotThreadSafe
public class ElasticWriter extends BaseOutputWriter {

	private static final Logger log = LoggerFactory.getLogger(ElasticWriter.class);

	private static final String INDEX_OPERATION_NAME = "index";
	private static final String INDEX_PARAM = "_index";
	private static final String TYPE_PARAM = "_type";
	private static final String BULK_ENDPOINT = "/_bulk";

	private static final String DEFAULT_ROOT_PREFIX = "jmxtrans";
	private static final String ELASTIC_TYPE_NAME = "doc";

	private HttpClient httpClient;
	private StringBuilder bulkBuilder;

	private final String rootPrefix;
	private final String connectionUrl;
	private final String indexName;

	private DateFormat dateFormat;

	@JsonCreator
	public ElasticWriter(
			@JsonProperty("typeNames") ImmutableList<String> typeNames,
			@JsonProperty("booleanAsNumber") boolean booleanAsNumber,
			@JsonProperty("rootPrefix") String rootPrefix,
			@JsonProperty("debug") Boolean debugEnabled,
			@JsonProperty("connectionUrl") String connectionUrl,
			@JsonProperty("username") String username,
			@JsonProperty("password") String password,
			@JsonProperty("settings") Map<String, Object> settings) throws IOException {

		super(typeNames, booleanAsNumber, debugEnabled, settings);

		this.rootPrefix = firstNonNull(
				rootPrefix,
				DEFAULT_ROOT_PREFIX,
				DEFAULT_ROOT_PREFIX);

		this.connectionUrl = connectionUrl;
		this.indexName = this.rootPrefix;
		this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	}


	@Override
	protected void internalWrite(Server server, Query query, ImmutableList<Result> results) throws Exception {

		for (Result result : results) {
			log.debug("Query result: [{}]", result);
			if (isNumeric(result.getValue())) {
				Map<String, Object> map = new HashMap<>();
				map.put("serverAlias", server.getAlias());
				map.put("server", server.getHost());
				map.put("port", server.getPort());
				map.put("objDomain", result.getObjDomain());
				map.put("className", result.getClassName());
				map.put("typeName", result.getTypeName());
				map.put("attributeName", result.getAttributeName());
				map.put("valuePath", Joiner.on('/').join(result.getValuePath()));
				map.put("keyAlias", result.getKeyAlias());
				map.put("value", Double.parseDouble(result.getValue().toString()));
				map.put("timestamp", result.getEpoch());

				log.debug("Insert into Elastic: Index: [{}] Type: [{}] Map: [{}]", indexName, ELASTIC_TYPE_NAME, map);
				Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
				Map<String, String> indexParameters = new HashMap<String, String>();
				indexParameters.put(INDEX_PARAM, indexName + getDate());
				indexParameters.put(TYPE_PARAM, ELASTIC_TYPE_NAME);
				parameters.put(INDEX_OPERATION_NAME, indexParameters);

				parameters.put(INDEX_OPERATION_NAME, indexParameters);
				JSONObject json = new JSONObject(map);
				synchronized (bulkBuilder) {
					bulkBuilder.append(JSONObject.toJSON(parameters));
					bulkBuilder.append("\n");
					bulkBuilder.append(json.toJSONString());
					bulkBuilder.append("\n");
				}

			} else {
				log.warn("Unable to submit non-numeric value to Elastic: [{}] from result [{}]", result.getValue(), result);
			}
		}

		String entity;
		synchronized (bulkBuilder) {
			entity = bulkBuilder.toString();
			bulkBuilder = new StringBuilder();
		}
		log.debug("Post Entity:" + entity);
		HttpPost httpRequest = new HttpPost(connectionUrl + BULK_ENDPOINT);
		httpRequest.setHeader("Content-Type", "application/x-ndjson");
		httpRequest.setEntity(new StringEntity(entity, "UTF-8"));
		HttpResponse response = httpClient.execute(httpRequest);
		int statusCode = response.getStatusLine().getStatusCode();
		log.debug("Status code from elasticsearch: " + statusCode);
		if (response.getEntity() != null) {
			log.debug("Status message from elasticsearch: " + EntityUtils.toString(response.getEntity(), "UTF-8"));
		}

		if (statusCode != HttpStatus.SC_OK) {
			if (response.getEntity() != null) {
				throw new ElasticWriterException(EntityUtils.toString(response.getEntity(), "UTF-8"));
			} else {
				throw new ElasticWriterException("Elasticsearch status code was: " + statusCode);
			}
		}
	}


	@Override
	public void start() throws LifecycleException {
		super.start();
		try {
			this.httpClient = HttpClients.createDefault();
			this.bulkBuilder = new StringBuilder();
		} catch (Exception e) {
			throw new LifecycleException("Failed to create elastic mapping.", e);
		}
	}

	@Override
	public void close() throws LifecycleException {
		super.close();
	}

	@Override
	public void validateSetup(Server server, Query query) throws ValidationException {
		// no validations
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("ElasticWriter{");
		sb.append("rootPrefix='").append(rootPrefix).append('\'');
		sb.append(", connectionUrl='").append(connectionUrl).append('\'');
		sb.append(", indexName='").append(indexName).append('\'');
		sb.append('}');
		return sb.toString();
	}

	private String getDate() {
		return dateFormat.format(new Date());
	}
}
