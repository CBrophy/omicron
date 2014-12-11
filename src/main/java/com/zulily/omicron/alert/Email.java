package com.zulily.omicron.alert;

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

@SuppressWarnings("UnusedDeclaration")
public final class Email {

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

    public Email build() {
      if (Iterables.isEmpty(to)) {
        throw new RuntimeException("No email recipients specified");
      }

      return new Email(
        this.smtpHost,
        this.smtpPort,
        this.from,
        this.to,
        Optional.fromNullable(this.authenticator)
      );
    }

  }

  private final Session smtpSession;
  private final Address from;
  private final ImmutableList<Address> recipients;


  public Email(final String smtpHost,
               final int smtpPort,
               final Address fromAddress,
               final Iterable<Address> recipients,
               final Optional<Authenticator> authenticator) {

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
    checkArgument(!Strings.isNullOrEmpty(message), "Cannnot send email with a null or empty message");


    MimeMessage mimeMessage = new MimeMessage(this.smtpSession);
    mimeMessage.setRecipients(Message.RecipientType.TO, Iterables.toArray(recipients, Address.class));
    mimeMessage.setFrom(this.from);
    mimeMessage.setSubject(subject);
    mimeMessage.setContent(message, "text/plain");
    Transport.send(mimeMessage);

  }

  public void sendHTML(final String subject, final String message) throws MessagingException {
    checkArgument(!Strings.isNullOrEmpty(subject), "Cannot send email with a null or empty subject");
    checkArgument(!Strings.isNullOrEmpty(message), "Cannnot send email with a null or empty message");


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
}
