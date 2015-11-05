package rcloud;

import uk.ac.ebi.rcloud.server.RServices;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.rcloud.rpf.RemoteLogListener;
import uk.ac.ebi.rcloud.rpf.ServantProvider;
import uk.ac.ebi.rcloud.rpf.ServantProviderFactory;
import uk.ac.ebi.rcloud.rpf.db.ServantProxyPoolSingletonDB;
import uk.ac.ebi.rcloud.rpf.exception.TimeoutException;
import uk.ac.ebi.rcloud.server.RType.RChar;
import uk.ac.ebi.rcloud.server.RType.RList;
import uk.ac.ebi.rcloud.server.RType.RNumeric;
import uk.ac.ebi.rcloud.server.RType.RObject;
import uk.ac.ebi.rcloud.server.callback.RAction;
import uk.ac.ebi.rcloud.server.callback.RActionConst;
import uk.ac.ebi.rcloud.server.callback.RActionListener;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Created by andrew on 31/07/2014.
 */
public class RCloudConnection {

    private static GenericObjectPool workerPool;
    private static volatile boolean isInitialized = false;
    private static final Logger log = LoggerFactory.getLogger(RCloudConnection.class);
    private static boolean validated = validateEnvironment();
    private final static String poolname = "EMIF_8G";

    private RServices rConnection = null;

    public RCloudConnection() {
        rConnection = createRServices(poolname);
    }

    public void close() {
        if (rConnection != null) {
            recycleRServices(rConnection);
        }
    }

    public static boolean inherits(RObject obj, String attr) {
        if (obj == null) {
            return false;
        }

        RList att = obj.getAttributes();
        if (att == null) {
            return false;
        }

        String[] names = att.getNames();
        for (int i=0;i<names.length;i++) {
            if ("class".equals(names[i])) {
                RObject cl = att.getValue()[i];
                if (cl != null && cl instanceof RChar) {
                    RChar clCh = (RChar) cl;
                    for (String s : clCh.getValue()) {
                        if (attr.equals(s)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    public static String getString(RObject obj) {
        if (obj != null && obj instanceof RChar) {
            String[] val = ((RChar)obj).getValue();
            if (val.length > 0) {
                return val[0];
            }
        }
        return null;
    }

    public void eval(String cmd) {
        if (rConnection != null) {
            try {
                rConnection.consoleSubmit(cmd);
            } catch (Exception ex) {
                log.error("Error! ", ex);
            }
        }
    }

    public RObject parseAndEval(String cmd) {
        try {
            return rConnection.getObject(cmd);
        } catch (Exception ex) {
            log.error("Error! ", ex);
        }
        return null;
    }

    public String readScript(String scriptfilename) {
        StringBuilder script = new StringBuilder();

        try {
            BufferedReader input = new BufferedReader(new FileReader(new File(scriptfilename)));
            try {
                String line = null;

                while ((line = input.readLine()) != null) {
                    script.append(line);
                    script.append("\n");
                }

            } finally {
                try {
                    input.close();
                } catch (IOException ioe) {
                }
            }

        } catch (FileNotFoundException fnfe) {
            log.error("Error!", fnfe);
        } catch (IOException ioe) {
            log.error("Error!", ioe);
        }

        return script.toString();
    }


    public void sourceScript(String scriptfilename) {
        if (rConnection != null) {
            String script = readScript(scriptfilename);
            try {
                rConnection.sourceFromBuffer(script);
            } catch (Exception ex) {
                log.error("Error! ", ex);
            }
        }
    }

    public static RServices createRServices(String poolname) {
        // lazily initialize servant provider
        initialize(poolname);

        log.trace("Worker pool before borrow: [active = {}, idle = {}]",
                workerPool.getNumActive(), workerPool.getNumIdle());
        try {
           return tryBorrowAWorker(5, 20, TimeUnit.SECONDS);
        }
        finally {
            log.trace("Worker pool after borrow: [active = {}, idle = {}]",
                    workerPool.getNumActive(), workerPool.getNumIdle());
        }
    }


    public static boolean validateEnvironment() {

        /*
        if (System.getProperty("pools.dbmode.host") == null) {
            log.error("pools.dbmode.host not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.port") == null) {
            log.error("pools.dbmode.port not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.name") == null) {
            log.error("pools.dbmode.name not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.user") == null) {
            log.error("biocep.dbmode.user not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.password") == null) {
            log.error("biocep.db.password not set");
            return false;
        }
        if (System.getProperty("naming.mode") == null) {
            log.error("biocep.naming.mode not set");
            return false;
        }
        if (System.getProperty("pools.provider.factory") == null) {
            log.error("biocep.provider.factory not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.type") == null) {
            log.error("biocep.db.type not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.driver") == null) {
            log.error("pools.dbmode.driver not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.defaultpool") == null) {
            log.error("pools.dbmode.defaultpool not set");
            return false;
        }
        if (System.getProperty("pools.dbmode.killused") == null) {
            log.error("pools.dbmode.killused not set");
            return false;
        }
        */

        /*
        try {
            RServices r = createRServices(poolname);
            recycleRServices(r);
        }
        catch (Throwable e) {
            log.error("Critical error while trying to get biocep R service - " +
                    "check biocep is installed and required database is present", e);
            return false;
        }
        */
        return true;
    }


    private static RServices tryBorrowAWorker(int attempts, long wait, TimeUnit timeUnit) {
        for (int i = 0; i < attempts; i++) {
            log.debug("Borrow attempt: {}", i);
            try {
                RServices rServices = (RServices) workerPool.borrowObject();
                log.debug("Borrowed {} from the pool", rServices.getServantName());
                return rServices;
            } catch (TimeoutException e) {
                log.debug("RServices pool is probably busy..", e);
                log.info("No free workers in the pool currently. Waiting for {} {}...", wait, timeUnit);
            } catch (Exception e) {
                log.error("Failed to borrow RServices worker", e);
                return null;
            }
            waitFor(timeUnit.toMillis(wait));
        }
        log.error("Could not borrow RServices after " + attempts + " attempts");
        return null;
    }

    private static void waitFor(long milliseconds) {
        final CountDownLatch latch = new CountDownLatch(1);

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                latch.countDown();
            }
        }, milliseconds);

        try {
            latch.await();
        } catch (InterruptedException e) {
            //Wake up!
        }
    }

    public static void recycleRServices(RServices rServices) {
        log.trace("Recycling R services");
        log.trace("Worker pool before return... " +
                "active = " + workerPool.getNumActive() + ", idle = " + workerPool.getNumIdle());
        try {
            if (rServices != null) {
                workerPool.returnObject(rServices);
                log.trace("Worker pool after return... " +
                        "active = " + workerPool.getNumActive() + ", idle = " + workerPool.getNumIdle());
            }
            else {
                log.error("R services object became unexpectedly null!");
            }
        }
        catch (Exception e) {
            log.error("Failed to release an RServices object back into the pool of workers", e);
        }
    }

    public static synchronized void releaseResources() {
        log.debug("Releasing resources...");
        if (isInitialized) {
            try {
                if (workerPool.getNumActive() > 0) {
                    log.warn("Shutting down even though there are still some active compute workers");
                }

                workerPool.clear();
                workerPool.close();
            }
            catch (Exception e) {
                log.error("Problem shutting down compute service", e);
            }
        }
    }

    private static synchronized void initialize(String poolname) {
        if (!isInitialized) {
            // create worker pool
            workerPool = new GenericObjectPool(new RWorkerObjectFactory(poolname));
            workerPool.setMaxActive(4);
            workerPool.setMaxIdle(4);
            workerPool.setTestOnBorrow(true);
            workerPool.setTestOnReturn(true);

            isInitialized = true;
        }
    }

    private static class RWorkerObjectFactory implements PoolableObjectFactory {
        private final String poolname;
        private final ServantProvider sp;

        public RWorkerObjectFactory(String poolname) {
            this.poolname = poolname;
            sp = ServantProviderFactory.getFactory().getServantProvider();
        }

        public synchronized Object makeObject() throws Exception {
            log.debug("Attempting to create another rServices object for the pool " + poolname + " ...");
            RServices rServices = (RServices) sp.borrowServantProxy(poolname);

            if (null == rServices) {
                log.debug("Borrowed an rServices object that proved to be null");
                throw new NoSuchElementException();
            }

            log.debug("rServices acquired, registering logging/console reporting listeners");

            // add output listener
            rServices.addRConsoleActionListener(new MyRConsoleActionListener());
            MyRemoteLogListener listener = new MyRemoteLogListener();
            rServices.addLogListener(listener);


            log.debug("Acquired biocep worker " + rServices.getServantName());
            return rServices;
        }

        public synchronized void destroyObject(Object o) throws Exception {
            RServices R = (RServices) o;
            log.debug("Released biocep worker " + R.getServantName());

            sp.returnServantProxy((RServices) o);
        }

        public synchronized boolean validateObject(Object o) {
            log.debug("Testing validity of " + o.toString() + "...");
            try {
                // check response to ping
                RServices rServices = (RServices) o;
                String pingResponse = rServices.ping();
                log.debug("Worker response to ping: " + pingResponse);

                // test this worker can evaluate 2 + 2
                boolean valid;
                try {
                    double[] values = ((RNumeric) rServices.getObject("2 + 2")).getValue();
                    if (values.length > 0) {
                        log.debug("Worker response to 2 + 2: " + values[0]);
                        valid = (values[0] == 4.0);
                    }
                    else {
                        log.debug("No response to 2 + 2 from worker");
                        valid = false;
                    }
                }
                catch (Exception e) {
                    log.error("R worker threw exception during validity test, invalidating");
                    valid = false;
                }

                if (!valid) {
                    log.warn("R worker " + rServices.getServantName() + " could not accurately evaluate 2 + 2 - " +
                            "this worker will be released");
                    // invalidate the worker if it's not valid
                    rServices.die();
                    return false;
                }
                else {
                    return true;
                }
            }
            catch (RemoteException e) {
                log.error("R worker does not respond to ping correctly ({}). Invalidated.", e);
                return false;
            }
            catch (Exception e) {
                log.error("Error.", e);
                return false;
            }
        }

        public synchronized void activateObject(Object o) throws Exception {
        }

        public synchronized void passivateObject(Object o) throws Exception {
        }
    }


    public static class MyRConsoleActionListener
            extends UnicastRemoteObject
            implements RActionListener, Serializable {
        private static final Logger log = LoggerFactory.getLogger(RCloudConnection.class);

        public MyRConsoleActionListener() throws RemoteException {
            super();
        }

        public void notify(RAction consoleAction) throws RemoteException {
            log.info(consoleAction.toString());
            //log.info("console:\n\t" + consoleAction.getAttributes().get(RActionConst.OUTPUT));
        }
    }

    public static class MyRemoteLogListener
            extends UnicastRemoteObject
            implements RemoteLogListener, Serializable {
        private static final Logger log = LoggerFactory.getLogger(RCloudConnection.class);

        public MyRemoteLogListener() throws RemoteException {
            super();
        }

        public void write(String text) throws RemoteException {
            log.info(text);
        }
    }

    private static void setSystemProperty(String name, String value) {
        String val = System.getProperty(name);

        if (val == null || val.equals("")){
            System.setProperty(name, value);
        }
    }

    public static void main(String[] args) {

        //RConnection c = new RConnection();
        RCloudConnection c = new RCloudConnection();


        //REXP x = c.eval(workingDirectoryCommand);
        c.eval("setwd('/ebi/microarray/home/biocep/service/VirtualRWorkbench/wdir/andrew/ArrayExpressHTSAnalysis')");


        c.sourceScript("/Users/andrew/project/tranSMART/test.R");


        //REXP r = c.parseAndEval("try("+reformattedCommand+",silent=TRUE)");
        String reformattedCommand = "z";
        RObject r = c.parseAndEval("try("+reformattedCommand+",silent=TRUE)");

        //if (r.inherits("try-error"))
        if (RCloudConnection.inherits(r, "try-error")) {
            //String rError = r.asString()
            String rError = RCloudConnection.getString(r);
            log.info("message: " + rError);
        }

        c.close();

        RCloudConnection.releaseResources();
    }


}

