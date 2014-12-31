/*
 * Copyright (C) 2014 zulily, Inc.
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
package com.zulily.omicron.alert;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.MediaType;

import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Arrays;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.zulily.omicron.Utils.info;

/**
 * Utility class used by {@link com.zulily.omicron.alert.AlertManager} to send {@link com.zulily.omicron.alert.Alert}
 * messages via email
 */
@SuppressWarnings("UnusedDeclaration")
final class EmailSender {

  public final static String EXAMPLE_ADDRESS = "someone@example.com";
  private final Session smtpSession;
  private final Address from;
  private final ImmutableList<Address> recipients;

  EmailSender(final String smtpHost,
              final int smtpPort,
              final Address fromAddress,
              final Iterable<Address> recipients,
              final Optional<Authenticator> authenticator) {

    checkNotNull(smtpHost, "smtpHost");
    checkArgument(smtpPort > 0, "smtpPort must be positive");

    Properties sessionProperties = System.getProperties();

    sessionProperties.setProperty("mail.smtp.host", smtpHost);
    sessionProperties.setProperty("mail.smtp.port", String.valueOf(smtpPort));

    if (authenticator.isPresent()) {
      sessionProperties.setProperty("mail.smtp.auth", "true");
      this.smtpSession = Session.getInstance(sessionProperties, authenticator.get());
    } else {
      sessionProperties.setProperty("mail.smtp.auth", "false");
      this.smtpSession = Session.getInstance(sessionProperties);
    }

    this.from = fromAddress;
    this.recipients = ImmutableList.copyOf(recipients);
  }

  public void send(final String subject, final String message) throws MessagingException {
    checkArgument(!Strings.isNullOrEmpty(subject), "Cannot send email with a null or empty subject");
    checkArgument(!Strings.isNullOrEmpty(message), "Cannot send email with a null or empty message");

    if (EXAMPLE_ADDRESS.equalsIgnoreCase(from.toString())) {
      dumpEmail(subject, message);
      // Allow testing to use a fake address
      return;
    }

    MimeMessage mimeMessage = new MimeMessage(this.smtpSession);
    mimeMessage.setRecipients(Message.RecipientType.TO, Iterables.toArray(recipients, Address.class));
    mimeMessage.setFrom(this.from);
    mimeMessage.setSubject(subject);
    mimeMessage.setContent(message, "text/plain");
    Transport.send(mimeMessage);

  }

  public void sendHTML(final String subject, final String message) throws MessagingException {
    checkArgument(!Strings.isNullOrEmpty(subject), "Cannot send email with a null or empty subject");
    checkArgument(!Strings.isNullOrEmpty(message), "Cannot send email with a null or empty message");

    if (EXAMPLE_ADDRESS.equalsIgnoreCase(from.toString())) {
      dumpEmail(subject, message);
      // Allow testing to use a fake address
      return;
    }

    MimeMessage mimeMessage = new MimeMessage(this.smtpSession);
    mimeMessage.setRecipients(Message.RecipientType.TO, Iterables.toArray(recipients, Address.class));
    mimeMessage.setFrom(this.from);
    mimeMessage.setSubject(subject);
    mimeMessage.setContent(formatMessage(message), MediaType.HTML_UTF_8.toString());
    Transport.send(mimeMessage);
  }

  private String formatMessage(final String message) {
    return "<div style=\"font-family: monospace; white-space: pre-wrap;\">" +
      message +
      "</div>";
  }

  private void dumpEmail(final String subject, final String message) {
    StringBuilder emailConfigBuilder = new StringBuilder("\n");

    emailConfigBuilder = emailConfigBuilder.append("#BEGIN ALERT EMAIL#").append("\n");
    emailConfigBuilder = emailConfigBuilder.append("smtpAuth: ").append(this.smtpSession.getProperty("mail.smtp.auth")).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("smtpHost: ").append(this.smtpSession.getProperty("mail.smtp.host")).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("smtpPort: ").append(this.smtpSession.getProperty("mail.smtp.port")).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("-----------------------").append("\n");
    emailConfigBuilder = emailConfigBuilder.append("to: ").append(Joiner.on(',').join(this.recipients)).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("from: ").append(this.from).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("subject: ").append(subject).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("message: ").append(message).append("\n");
    emailConfigBuilder = emailConfigBuilder.append("#END ALERT EMAIL#").append("\n");

    info(emailConfigBuilder.toString());
  }

  private static javax.mail.Authenticator buildAuthenticator(final String username, final String password) {

    return new Authenticator() {

      public PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(username, password);
      }

    };

  }

  public static Builder from(final String address) throws AddressException {

    checkArgument(!Strings.isNullOrEmpty(address), "address cannot be empty");

    return new Builder(new InternetAddress(address));
  }

  public static final class Builder {

    private final Address from;
    private Iterable<Address> to = Lists.newArrayList();
    private String smtpHost = "localhost";
    private int smtpPort = 25;
    private Authenticator authenticator = null;

    private Builder(InternetAddress from) throws AddressException {
      this.from = checkNotNull(from, "from");
    }

    public Builder to(String address) throws AddressException {
      checkArgument(!Strings.isNullOrEmpty(address), "address cannot be empty");
      this.to = Arrays.<Address>asList(new InternetAddress(address));
      return this;
    }

    public Builder to(Iterable<String> addresses) throws AddressException {
      checkArgument(!Iterables.isEmpty(addresses), "addresses cannot be empty");

      ImmutableList.Builder<Address> recipientBuilder = ImmutableList.builder();

      for (String recipient : addresses) {
        recipientBuilder.add(new InternetAddress(recipient));
      }

      this.to = recipientBuilder.build();
      return this;
    }

    public Builder withSMTPServer(String hostname, int port) {
      checkArgument(!Strings.isNullOrEmpty(hostname), "hostname cannot be empty");
      checkArgument(port > 0, "port must be > 0");

      this.smtpHost = hostname;
      this.smtpPort = port;

      return this;
    }

    public Builder withAuthentication(String username, String password) {

      checkArgument(!Strings.isNullOrEmpty(username), "username cannot be empty");
      checkArgument(!Strings.isNullOrEmpty(password), "password cannot be empty");

      this.authenticator = buildAuthenticator(username, password);

      return this;
    }

    public EmailSender build() {

      checkState(!Iterables.isEmpty(to), "No email recipients specified");
      checkState(from != null, "No from address specified");
      checkState(!Strings.isNullOrEmpty(this.smtpHost), "No smtp host specified");
      checkState(this.smtpPort > 0, "No smtp port specified");

      return new EmailSender(
        this.smtpHost,
        this.smtpPort,
        this.from,
        this.to,
        Optional.fromNullable(this.authenticator)
      );
    }

  }

}
