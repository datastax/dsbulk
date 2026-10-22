# Set up Kerberos client authentication

When loading data into a Kerberos-enabled cluster, DSBulk must provide Kerberos credentials using a keytab file or a ticket cache.

## Configure the location of the Kerberos Configuration file

Set the location of the Kerberos configuration file when it is not in the default location.

Authenticating with Kerberos credentials using a keytab file or ticket cache requires the Kerberos configuration file (`krb5.conf`).
Typically, this file is in the `/etc` directory.
If it is not there, obtain one from your Kerberos system administrator.

If the Kerberos configuration file is not the default location (`/etc`), set the environment variables for Kerberos command line tools, such as `kinit`, `klist`, and `kdestroy` and `dsbulk`.

1. Set the `KRB5_CONFIG` environment variable to the location of `krb5.conf`.
   For example:

   ```bash
   export KRB5_CONFIG=$JAVA_HOME/lib/security/krb5.conf
   ```

2. Add the path to `DSBULK_JAVA_OPTS`.
   For example:

   ```bash
   export DSBULK_JAVA_OPTS="$DSBULK_JAVA_OPTS -Djava.security.krb5.conf=$JAVA_HOME/lib/security/krb5.conf"
   ```

## Use a Kerberos Keytab file for authentication

Use a keytab file to get credentials for authentication with a DSE cluster.

To use a Kerberos keytab file, use the `kadmin` command to create the keytab file and get a ticket.

1. Install [kinit](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) and [kadmin](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/admin_commands/kadmin_local.html).

2. Create a keytab file with `kadmin`:

   1. Start `kadmin`:

      ```bash
      kadmin
      ```

   2. Create file:

      ```bash
      ktadd -k file_name principal_name
      ```

   3. Authenticate using `kinit`:

      ```bash
      kinit -k -t file_name principal_name
      ```
3. Configure DSBulk to use the Kerberos Keytab file for authentication:

   - **Configure Kerberos Keytab file authentication on the command line**:

      Specify Kerberos options at runtime on the command line:

      ```bash
      dsbulk load -k ks -t t1 -url ~/data.csv \
      --driver.advanced.auth-provider.class DseGssApiAuthProvider \
      --driver.advanced.auth-provider.login-configuration.principal dsbulk_principal_name \
      --driver.advanced.auth-provider.login-configuration.keyTab file_path
      ```

   - **Configure Kerberos Keytab file authentication in `application.conf`**:

      Set the following parameters in `application.conf`:

      - [`driver.advanced.auth-provider.class`](options/driver-options.md#datastaxJavaDriverAdvancedAuthProviderClass) to `DseGssApiAuthProvider`.
      - `driver.advanced.auth-provider.login-configuration.principal` to the principal name.
      - `driver.advanced.auth-provider.login-configuration.keyTab` to keytab file using the full path.

      If multiple principals may have valid tickets in the ticket cache, DSBulk arbitrarily chooses one to use.
      You can specify the principal explicitly by setting the `driver.advanced.auth-provider.login-configuration.principal` to the principal name.
      For example:

      
      ############ MyConfFile.conf ############

      dsbulk {
         # The name of the connector to use
         connector.name = "csv"
         # CSV field delimiter
         connector.csv.delimiter = "|"
         # The keyspace to connect to
         schema.keyspace = "myKeyspace"
         # The table to connect to
         schema.table = "myTable"
         # The field-to-column mapping
         schema.mapping = "0=name, 1=age, 2=email"
         # The authentication configuration for Kerberos
         driver.advanced.auth-provider.class="DseGssApiAuthProvider"
         driver.advanced.auth-provider.login-configuration.principal="principal_name"
         driver.advanced.auth-provider.login-configuration.keyTab="file_path"
      }
      ```

      > [!TIP]
      > When Kerberos authentication is fully configured in `application.conf`, you don't need to pass any additional command line parameters to use Kerberos authentication.

## Use a Kerberos Ticket Cache for authentication

Use a ticket cache to authenticate with a DSE cluster.

To use the Kerberos ticket cache, first use the `kinit` command to authenticate with the Kerberos server and obtain a ticket.

1. Install [kinit](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) and [klist](https://web.mit.edu/kerberos/krb5-latest/doc/user/user_commands/klist.html).

2. Get a Kerberos ticket:

   1. Authenticate with the Kerberos server and obtain a ticket:

      ```bash
      kinit principal_name@REALM
      ```

   2. Verify the ticket and expiration:

      ```bash
      klist
      ```

      Returns a list of tickets with their expiration time:

      ```console
      Ticket cache: FILE:/tmp/krb5cc_1002
      Default principal: principal_name@REALM

      Valid starting       Expires              Service principal
      02/14/2020 21:53:51  02/15/2020 07:53:51  krbtgt/host@REALM
      	renew until 02/15/2020 21:53:49
      ```
3. Configure DSBulk to use the Kerberos ticket cache for authentication:

   - **Configure Kerberos ticket cache authentication on the command line**:

      Specify Kerberos options at runtime on the command line:

      - Use any cached ticket:

         ```bash
         dsbulk load -k ks -t t1 -url ~/data.csv \
            --driver.advanced.auth-provider.class DseGSSAPIAuthProvider
         ```

      - Use a specific principal if multiple tickets are cached:

         ```bash
         dsbulk load -k ks -t t1 -url ~/data.csv \
            --driver.advanced.auth-provider.class DseGSSAPIAuthProvider \
            --driver.advanced.auth-provider.login-configuration.principal dsbulk_principal_name
         ```

   - **Configure Kerberos ticket cache authentication in `application.conf`**:

      In `application.conf`, set [`driver.advanced.auth-provider.class`](options/driver-options.md#datastaxJavaDriverAdvancedAuthProviderClass) to `DseGSSAPIAuthProvider`.

      If multiple principals may have valid tickets in the ticket cache, DSBulk arbitrarily chooses one to use.
      You can specify the principal explicitly by setting the `driver.advanced.auth-provider.login-configuration.principal` to the principal name.
      For example:

      ```json
      ############ MyConfFile.conf ############

      dsbulk {
         # The name of the connector to use
         connector.name = "csv"
         # CSV field delimiter
         connector.csv.delimiter = "|"
         # The keyspace to connect to
         schema.keyspace = "myKeyspace"
         # The table to connect to
         schema.table = "myTable"
         # The field-to-column mapping
         schema.mapping = "0=name, 1=age, 2=email"
         # The authentication provider for Kerberos
         driver.advanced.auth-provider.class="DseGSSAPIAuthProvider"
         driver.advanced.auth-provider.login-configuration.principal="principal_name"
      }
      ```

      > [!TIP]
      > When Kerberos authentication is fully configured in `application.conf`, you don't need to pass any additional command line parameters to use Kerberos authentication.
