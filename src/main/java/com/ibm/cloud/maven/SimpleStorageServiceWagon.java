/*
 * Copyright 2010-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.cloud.maven;

import com.ibm.cloud.objectstorage.AmazonServiceException;
import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.*;

import org.apache.maven.wagon.AbstractWagon;
import org.apache.maven.wagon.ConnectionException;
import org.apache.maven.wagon.ResourceDoesNotExistException;
import org.apache.maven.wagon.TransferFailedException;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authorization.AuthorizationException;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.resource.Resource;

import java.io.*;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

/**
 * IBM Cloud COS maven wagon.
 */
public final class SimpleStorageServiceWagon extends AbstractWagon 
{

    private volatile AmazonS3 amazonS3;

    private volatile String endpoint;
    private volatile String bucketName;
    private volatile String baseDirectory;

    @Override
    protected void openConnectionInternal()
      throws ConnectionException, AuthenticationException
    {
      var credentialsProvider = 
        new AuthenticationInfoAWSCredentialsProviderChain(getAuthenticationInfo());
      var clientConfiguration = new ClientConfiguration();
      var proxyInfo = getProxyInfo("cos", getRepository().getHost());

      if (proxyInfo != null)
      {
        clientConfiguration.
          withProxyHost(proxyInfo.getHost()).
          withProxyPort(proxyInfo.getPort());
      }

      this.endpoint = getRepository().getHost();
      
      var path = Objects.requireNonNull(repository.getBasedir()).
        replace('\\', '/');
      
      if (path.endsWith("/"))
      {
        path = path.substring(0, path.length() - 1);
      }
      
      if (path.startsWith("/"))
      {
        path = path.substring(1);
      }
      
      var p = path.indexOf('/');
      
      if (p == -1)
      {
        this.bucketName = path;
        this.baseDirectory = "/";
      }
      else
      {
        this.bucketName = path.substring(0, p);
        this.baseDirectory = path.substring(p + 1) + "/";
      }
      
      this.amazonS3 = AmazonS3ClientBuilder.standard().
        withCredentials(credentialsProvider).
        withClientConfiguration(clientConfiguration).
        withEndpointConfiguration(
          new EndpointConfiguration(endpoint, null)).
        withPathStyleAccessEnabled(true).
        build();
    }

    @Override
    protected void closeConnection() throws ConnectionException
    {
      amazonS3 = null; 
    }
    
    @Override
    public void get(String resourceName, File destination) 
      throws TransferFailedException, ResourceDoesNotExistException, AuthorizationException 
    {
      var key = createS3Key(resourceName);
      var resource = new Resource(resourceName);

      fireGetInitiated(resource, destination);
      fireGetStarted(resource, destination);
      
      try
      {
        var metadata = 
          amazonS3.getObject(new GetObjectRequest(bucketName, key), destination);
          
        if (metadata != null)
        {
          resource.setContentLength(metadata.getContentLength());
          resource.setLastModified(
            metadata.getLastModified().toInstant().toEpochMilli());
        }
      }
      catch(AmazonServiceException e)
      {
        fireTransferError(resource, e, TransferEvent.REQUEST_GET);

        if ("NoSuchKey".equals(e.getErrorCode()))
        {
          throw new ResourceDoesNotExistException(
            "Resource " + resourceName + " does not exist in the repository", 
            e);
        }

        throw e;
      }

      fireGetCompleted(resource, destination);
    }

    public void put(File source, String resourceName) 
      throws TransferFailedException, AuthorizationException 
    {
      var key = createS3Key(resourceName);
      var resource = new Resource(resourceName);
      
      resource.setContentLength(source.length());
      resource.setLastModified(source.lastModified());

      firePutInitiated(resource, source);
      firePutStarted(resource, source);

      amazonS3.putObject(bucketName, key, source);

      firePutCompleted(resource, source);
    }

    @Override
    public boolean resourceExists(String resourceName) 
      throws TransferFailedException, AuthorizationException 
    {
      var key = createS3Key(resourceName);

      try 
      {
        amazonS3.getObjectMetadata(bucketName, key);
        
        return true;
      } 
      catch(Exception e) 
      {
          return false;
      }
    }

    @Override
    public boolean getIfNewer(String resourceName, File destination,
      long timestamp) throws TransferFailedException,
      ResourceDoesNotExistException, AuthorizationException
    {
      var key = createS3Key(resourceName);
      var resource = new Resource(resourceName);

      fireGetInitiated(resource, destination);
      fireGetStarted(resource, destination);
      
      var lastModified = Date.from(Instant.ofEpochMilli(timestamp));
      
      try
      {
        var metadata = amazonS3.getObject(
          new GetObjectRequest(bucketName, key).
            withModifiedSinceConstraint(lastModified), 
          destination);
          
        if (metadata != null)
        {
          resource.setContentLength(metadata.getContentLength());
          resource.setLastModified(
            metadata.getLastModified().toInstant().toEpochMilli());
        }
        
        fireGetCompleted(resource, destination);

        return metadata == null || 
          metadata.getLastModified().compareTo(lastModified) > 0;
      }
      catch(Exception e)
      {
        return false;
      }
    }
    
    private String createS3Key(String resourceName)
    {
      return baseDirectory + 
        Objects.requireNonNull(resourceName).replace('\\', '/');
    }
}
