/**
 * Copyright C 2020 OpsHub, Inc. All rights reserved
 */
package com.opshub.eai.gitalm.adapter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.opshub.eai.EAIAttachment;
import com.opshub.eai.core.adapters.OIMBaseAttachmentImpl;
import com.opshub.eai.core.exceptions.OIMAdapterException;
import com.opshub.eai.gitalm.entities.GithubConnectorService;
import com.opshub.eai.metadata.AttachmentMeta;

/**
 * This class handles all the attachment related functionality in Github
 * connector. This class is implemented to handle inline image and inline
 * document in html fields of Pull Request entity.
 */
public class GithubAttachmentImpl extends OIMBaseAttachmentImpl {

	private static final Logger LOGGER = Logger.getLogger(GithubAttachmentImpl.class);
	private GithubConnectorService connectorService;

	public GithubAttachmentImpl(final GithubConnectorService connectorService) {
		this.connectorService = connectorService;
	}

	@Override
	public EAIAttachment addAttachment(final String internalId, final InputStream byteInputStream, final String label, final String fileName,
			final String contentType, final String addedByUser, final Map<String, Object> additionalParameters)
			throws OIMAdapterException {
		LOGGER.trace("GitHub write operation is not supported. It only supports polling");
		throw new UnsupportedOperationException("Adding attachment is not supported for Github");
	}

	@Override
	public void deleteAttachment(final String internalId, final EAIAttachment attachment) throws OIMAdapterException {
		LOGGER.trace("GitHub write operation is not supported.");
		throw new UnsupportedOperationException("Deleting attachment is not supported for Github");
	}

	@Override
	public EAIAttachment updateAttachment(final String internalId, final EAIAttachment attachment,
			final InputStream byteInputStream)
			throws OIMAdapterException {
		LOGGER.trace("Any write operation in GitHub is not supported.");
		throw new UnsupportedOperationException("Update attachment is not supported for Github");
	}

	/**
	 * Returning empty list as no attachments can be added in GitHub. We are
	 * polling inline images and documents only
	 */
	@Override
	public List<EAIAttachment> getCurrentAttachments(final String internalId, final Object entityObject)
			throws OIMAdapterException {
		return new ArrayList<>();
	}

	/**
	 * Returning attachment meta for only source as true as inline image and
	 * document will get polled in case of Github pull request entity
	 */
	@Override
	public AttachmentMeta getAttachementMeta() {
		return connectorService.getGithubEntityHandler().getAttachementMeta();
	}

	@Override
	public InputStream getAttachmentInputStream(final String attachmentURI) throws OIMAdapterException {
		return connectorService.getGithubEntityHandler().getAttachmentInputStream(attachmentURI);
	}

}
