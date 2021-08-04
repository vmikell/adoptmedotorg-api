const AWS = require('aws-sdk')
AWS.config.update({
  region: 'us-east-1',
})
const dynamodb = new AWS.DynamoDB.DocumentClient()
const dynamodbTableName = 'video'
const healthPath = '/health'
const urlPath = '/url'
const urlsPath = '/urls'

exports.handler = async function (event) {
  console.log('Request event: ', event)
  let response
  switch (true) {
    case event.httpMethod === 'GET' && event.path === healthPath:
      response = buildResponse(200)
      break
    case event.httpMethod === 'GET ' && event.path === urlPath:
      response = getUrl(event.queryStringParameters.urlId)
      break
    case event.httpMethod === 'GET' && event.path === urlsPath:
      response = await getUrls()
    case event.httpMethod === 'POST' && event.path === urlPath:
      response = await saveUrl(JSON.parse(event.body))
      break
    case event.httpMethod === 'PATCH' && event.path === urlPath:
      const requestBody = JSON.parse(event.body)
      response = await modifyUrl(
        requestBody.urlId,
        requestBody.updateKey,
        requestBody.updateValue
      )
      break
    case event.httpMethod === 'DELETE' && event.path === urlPath:
      response = await deleteUrl(JSON.parse(event.body).urlId)
      break
  }
  return response
}

async function getUrl(urlId) {
  const params = {
    TableName: dynamodbTableName,
    Key: {
      urlId: urlId,
    },
  }
  return await dynamodb
    .get(params)
    .promise()
    .then(
      (response) => {
        return buildResponse(200, response.Item)
      },
      (error) => {
        console.error(error)
      }
    )
}

async function getUrls() {
  const params = {
    TableName: dynamodbTableName,
  }
  const allurls = await scanDynamoRecords(params, [])
  const body = {
    urls: allurls,
  }
  return buildResponse(200, body)
}

async function scanDynamoRecords(scanParams, itemArray) {
  try {
    const dynamoData = await dynamodb.scan(scanParams).promise()
    itemArray = itemArray.concat(dynamoData.Items)
    if (dynamoData.LastEvaluatedKey) {
      scanParams.ExclusiveStartKey = dynamoData.LastEvaluatedKey
      return await scanDynamoRecords(scanParams, itemArray)
    }
    return itemArray
  } catch (error) {
    console.error(error)
  }
}

async function saveUrl(requestBody) {
  const params = {
    TableName: dynamodbTableName,
    Item: requestBody,
  }
  return await dynamodb
    .put(params)
    .promise()
    .then(
      () => {
        const body = {
          Operation: 'SAVE',
          Message: 'SUCCESS',
          Item: requestBody,
        }
        return buidResponse(200, body)
      },
      (error) => {
        console.error(error)
      }
    )
}

async function modifyUrl(urlID, updateKey, updateValue) {
  const params = {
    TableName: dynamodbTableName,
    Key: {
      urlId: urlId,
    },
    UpdateExpression: `set ${updateKey} = :value`,
    ExpressionAttributeValues: {
      ':value': updateValue,
    },
    ReturnValues: 'UPDATED_NEW',
  }
  return await dynamodb
    .update(params)
    .promise()
    .then(
      (response) => {
        const body = {
          Operation: 'UPDATE',
          Message: 'SUCCESS',
          Item: response,
        }
        return buildResponse(200, body)
      },
      (error) => {
        console.error(error)
      }
    )
}

async function deleteurl(url) {
  const params = {
    TableName: dynamodbTableName,
    Key: {
      urlId: urlId,
    },
    ReturnValues: 'ALL_OLD',
  }
  return await dynamodb
    .delete(params)
    .promise()
    .then(
      (response) => {
        const body = {
          Operation: 'DELETE',
          Message: 'SUCCESS',
          Item: response,
        }
        return buildResponse(200, body)
      },
      (error) => {
        console.error(error)
      }
    )
}

function buildResponse(statusCode, body) {
  return {
    statusCode: statusCode,
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  }
}
