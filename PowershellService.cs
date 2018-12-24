using Microsoft.PowerShell;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Management.Automation;
using System.Management.Automation.Host;
using System.Management.Automation.Runspaces;
using System.Runtime.InteropServices;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using static System.Diagnostics.EventLog;
using static PSService.ServiceState;

namespace PSService
{
    public enum ServiceType
    {
        ServiceWin32OwnProcess = 0x00000010,
        ServiceWin32ShareProcess = 0x00000020,
    }

    public enum ServiceState
    {
        ServiceStopped = 0x00000001,
        ServiceStartPending = 0x00000002,
        ServiceStopPending = 0x00000003,
        ServiceRunning = 0x00000004,
        ServiceContinuePending = 0x00000005,
        ServicePausePending = 0x00000006,
        ServicePaused = 0x00000007
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct ServiceStatus
    {
        public ServiceType dwServiceType;
        public ServiceState dwCurrentState;
        public int dwControlsAccepted;
        public int dwWin32ExitCode;
        public int dwServiceSpecificExitCode;
        public int dwCheckPoint;
        public int dwWaitHint;
    }

    public enum Win32Error
    {
        NoError = 0,
        ErrorAppInitFailure = 575,
        ErrorFatalAppExit = 713,
        ErrorServiceNotActive = 1062,
        ErrorExceptionInService = 1064,
        ErrorServiceSpecificError = 1066,
        ErrorProcessAborted = 1067
    }

    /// <summary>
    /// event argument for power event handler
    /// </summary>
    public class PowerEventArgs : EventArgs
    {
        internal PowerEventArgs(PowerBroadcastStatus powerStatus)
        {
            PowerStatus = powerStatus;
        }

        public PowerBroadcastStatus PowerStatus { get; }
    }

    public class SessionChangeEventArgs : EventArgs
    {
        internal SessionChangeEventArgs(SessionChangeReason reason, int sessionId)
        {
            Reason = reason;
            SessionId = sessionId;
        }

        public SessionChangeReason Reason { get; }
        public int SessionId { get; }
    }

    public class CustomCommandEventArgs : EventArgs
    {
        internal CustomCommandEventArgs(int command)
        {
            Command = command;
        }

        public int Command { get; }
    }

    public class PowershellService : ServiceBase
    {
        protected class ServiceEvents
        {
            protected PowershellService Service;

            public ServiceEvents(PowershellService service)
            {
                Service = service;
            }

            private readonly object _powerLock = new object();
            private readonly object _sessionChangeLock = new object();
            private readonly object _pauseLock = new object();
            private readonly object _continueLock = new object();
            private readonly object _shutdownLock = new object();
            private readonly object _stopLock = new object();
            private readonly object _customCommandLock = new object();

            public event EventHandler<PowerEventArgs> Power
            {
                add
                {
                    lock (_powerLock)
                    {
                        Service.Power += value;
                    }
                }
                remove
                {
                    lock (_powerLock)
                    {
                        Service.Power -= value;
                    }
                }
            }

            public event EventHandler<SessionChangeEventArgs> SessionChange
            {
                add
                {
                    lock (_sessionChangeLock)
                    {
                        Service.SessionChange += value;
                    }
                }
                remove
                {
                    lock (_sessionChangeLock)
                    {
                        Service.SessionChange -= value;
                    }
                }
            }

            public event EventHandler<EventArgs> Pause
            {
                add
                {
                    lock (_pauseLock)
                    {
                        Service.Pause += value;
                    }
                }
                remove
                {
                    lock (_pauseLock)
                    {
                        Service.Pause -= value;
                    }
                }
            }

            public event EventHandler<EventArgs> Continue
            {
                add
                {
                    lock (_continueLock)
                    {
                        Service.Continue += value;
                    }
                }
                remove
                {
                    lock (_continueLock)
                    {
                        Service.Continue -= value;
                    }
                }
            }

            public event EventHandler<EventArgs> Shutdown
            {
                add
                {
                    lock (_shutdownLock)
                    {
                        Service.Shutdown += value;
                    }
                }
                remove
                {
                    lock (_shutdownLock)
                    {
                        Service.Shutdown -= value;
                    }
                }
            }

            public event EventHandler<EventArgs> Stop
            {
                add
                {
                    lock (_stopLock)
                    {
                        Service.StopEvent += value;
                    }
                }
                remove
                {
                    lock (_stopLock)
                    {
                        Service.StopEvent -= value;
                    }
                }
            }

            public event EventHandler<CustomCommandEventArgs> CustomCommand
            {
                add
                {
                    lock (_customCommandLock)
                    {
                        Service.CustomCommand += value;
                    }
                }
                remove
                {
                    lock (_customCommandLock)
                    {
                        Service.CustomCommand -= value;
                    }
                }
            }
        }

        protected class ServiceHost : PSHost
        {
            protected PowershellService Service;
            protected PSObject PData;

            public ServiceHost(PowershellService service)
            {
                Service = service;
                PData = new PSObject();
                PData.Members.Add(new PSNoteProperty("Events", new ServiceEvents(service)));
            }

            /// <summary>
            /// hosting application's identification in user friendly fashion
            /// </summary>
            public override string Name
            {
                get { return HostName; }
            }

            /// <summary>
            /// version of the hosting application
            /// </summary>
            public override Version Version { get; } = new Version(HostVersion);

            /// <summary>
            /// GUID uniquely identifying this host instance
            /// </summary>
            public override Guid InstanceId { get; } = Guid.NewGuid();

            /// <summary>
            /// hosting application's implementation of PSHostUserInterface.
            /// No user interaction => null
            /// </summary>
            public override PSHostUserInterface UI => null;

            public override CultureInfo CurrentCulture { get; } = Thread.CurrentThread.CurrentCulture;

            public override CultureInfo CurrentUICulture { get; } = Thread.CurrentThread.CurrentUICulture;

            public override void EnterNestedPrompt()
            {
                throw new NotImplementedException("Nested prompts are not supported!");
            }

            public override void ExitNestedPrompt()
            {
                throw new NotImplementedException("Nested prompts are not supported!");
            }

            public override void NotifyBeginApplication()
            {
                // nothing to do
            }

            public override void NotifyEndApplication()
            {
                // nothing to do
            }

            public override void SetShouldExit(int exitCode)
            {
                WriteEntry(Service.ServiceName, string.Format("SetShouldExit({0}) was called!", exitCode));
                Service.ExitCodeByScript = exitCode;
                Task.Run(() => { Service.Stop(); });
            }

            public override PSObject PrivateData => PData;
        }

        // not inherited variables filled by application settings in xml config file
        /// <summary>
        /// if the system requests permission to suspend this value is returned. It can be changed by the PowerShell script.
        /// </summary>
        public bool AllowSuspend { get; set; }

        /// <summary>
        /// true if the script has to be notified about power events
        /// </summary>
        public bool ScriptCanHandlePowerEvent { get; set; }

        /// <summary>
        /// path to script file
        /// </summary>
        protected string ScriptFilePath;

        /// <summary>
        /// log name used for windows event log
        /// </summary>
        protected string LogName;

        protected int ServiceStartTimeout;

        protected int ServiceStopTimeout;

        protected int ServicePauseTimeout;

        protected int ServiceContinueTimeout;

        protected int ScriptStopTimeout;

        /// <summary>
        /// service status instance for reporting to service base
        /// </summary>
        protected ServiceStatus ServiceStatus;

        /// <summary>
        /// customized Powershell host
        /// </summary>
        protected ServiceHost PsHost;

        /// <summary>
        /// used instance of PowerShell class where script runs in
        /// </summary>
        protected PowerShell PowerShellInstance;

        /// <summary>
        /// result object of asynchronous PowerShell execution
        /// </summary>
        protected IAsyncResult AsyncPowerShell;

        /// <summary>
        /// exit code set by script (default 0)
        /// </summary>
        protected int ExitCodeByScript;

        // PowerShell host name and version
        const string HostName = "ServiceHost";
        const string HostVersion = "1.0";

        // keys for event support used in config file
        const string CanHandlePowerEventKeyConst = "CanHandlePowerEvent";
        const string CanHandleSessionChangeEventKeyConst = "CanHandleSessionChangeEvent";
        const string CanPauseAndContinueKeyConst = "CanPauseAndContinue";
        const string CanShutdownKeyConst = "CanShutdown";
        const string CanStopKeyConst = "CanStop";

        // further keys in config file
        const string AllowSuspendKeyConst = "AllowSuspend";
        const string AutoLogKeyConst = "AutoLog";
        const string LogNameKeyConst = "LogNameKey";
        const string ServiceNameKeyConst = "ServiceName";
        const string ScriptFilePathKeyConst = "ScriptFilePath";
        const string ServiceStartTimeoutKeyConst = "ServiceStartTimeout";
        const string ServiceStopTimeoutKeyConst = "ServiceStopTimeout";
        const string ServicePauseTimeoutKeyConst = "ServicePauseTimeout";
        const string ServiceContinueTimeoutKeyConst = "ServiceContinueTimeout";
        const string ScriptStopTimeoutKeyConst = "ScriptStopTimeout";

        // event definitions
        public event EventHandler<PowerEventArgs> Power;

        public event EventHandler<SessionChangeEventArgs> SessionChange;

        public event EventHandler<EventArgs> Pause;

        public event EventHandler<EventArgs> Continue;

        public event EventHandler<EventArgs> Shutdown;

        public event EventHandler<EventArgs> StopEvent;

        public event EventHandler<CustomCommandEventArgs> CustomCommand;

        internal event EventHandler<PSInvocationStateChangedEventArgs> PowerShellMonitor;

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus serviceStatus);

        public PowershellService()
        {
            InitFromAppSettings(false);

            // script has to be restarted when computer was shut down and restarted with fast startup (kernel hibernation)
            // so we need this feature internally
            CanHandlePowerEvent = true;

            ServiceStatus.dwServiceType = ServiceType.ServiceWin32OwnProcess;

            if (!SourceExists(ServiceName))
            {
                CreateEventSource(ServiceName, LogName);
            }

            WriteEntry(ServiceName, string.Format("Service {0} loaded", ServiceName));
        }

        protected override bool OnPowerEvent(PowerBroadcastStatus powerStatus)
        {
            try
            {
                // if script can handle power events, forward event
                if (ScriptCanHandlePowerEvent)
                {
                    Power?.Invoke(this, new PowerEventArgs(powerStatus));
                }
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, string.Format("{0} threw an exception: {1}", ServiceName, e),
                    EventLogEntryType.Error);
            }

            if (powerStatus == PowerBroadcastStatus.QuerySuspend)
            {
                return AllowSuspend;
            }

            return true;
        }

        protected override void OnSessionChange(SessionChangeDescription changeDescription)
        {
            try
            {
                SessionChange?.Invoke(this,
                    new SessionChangeEventArgs(changeDescription.Reason, changeDescription.SessionId));
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, $"{ServiceName} threw an exception: {e}", EventLogEntryType.Error);
            }
        }

        protected override void OnPause()
        {
            try
            {
                ServiceStatus.dwCurrentState = ServicePausePending;
                ServiceStatus.dwWin32ExitCode = 0;
                ServiceStatus.dwWaitHint = ServicePauseTimeout;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);

                Pause?.Invoke(this, EventArgs.Empty);

                ServiceStatus.dwCurrentState = ServicePaused;
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, $"{ServiceName} threw an exception: {e}", EventLogEntryType.Error);
                ServiceStatus.dwWin32ExitCode = (int) Win32Error.ErrorExceptionInService;
            }
            finally
            {
                ServiceStatus.dwWaitHint = 0;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);
            }
        }

        protected override void OnContinue()
        {
            try
            {
                ServiceStatus.dwCurrentState = ServiceContinuePending;
                ServiceStatus.dwWin32ExitCode = 0;
                ServiceStatus.dwWaitHint = ServiceContinueTimeout;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);

                Continue?.Invoke(this, EventArgs.Empty);

                ServiceStatus.dwCurrentState = ServiceRunning;
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, $"{ServiceName} threw an exception: {e}", EventLogEntryType.Error);
                ServiceStatus.dwWin32ExitCode = (int) Win32Error.ErrorExceptionInService;
            }
            finally
            {
                ServiceStatus.dwWaitHint = 0;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);
            }
        }

        protected override void OnShutdown()
        {
            try
            {
                Shutdown?.Invoke(this, EventArgs.Empty);
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, $"{ServiceName} threw an exception: {e}", EventLogEntryType.Error);
            }
        }

        protected override void OnCustomCommand(int command)
        {
            try
            {
                CustomCommand?.Invoke(this, new CustomCommandEventArgs(command));
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, string.Format("{0} threw an exception: {1}", ServiceName, e),
                    EventLogEntryType.Error);
            }
        }

        protected override void OnStart(string[] args)
        {
            ServiceStatus.dwCurrentState = ServiceStartPending;
            ServiceStatus.dwWin32ExitCode = 0;
            ServiceStatus.dwWaitHint = ServiceStartTimeout;
            SetServiceStatus(ServiceHandle, ref ServiceStatus);

            try
            {
                // Initialize Powershell environment
                InitialSessionState initialSessionState = InitialSessionState.CreateDefault();
                initialSessionState.ApartmentState = ApartmentState.STA;
                initialSessionState.ExecutionPolicy = ExecutionPolicy.Bypass;
                initialSessionState.ThreadOptions = PSThreadOptions.UseNewThread;

                // Initialize Powershell host
                PsHost = new ServiceHost(this);

                PowerShellInstance = PowerShell.Create();
                Runspace runspace = RunspaceFactory.CreateRunspace(PsHost, initialSessionState);
                runspace.Open();
                PowerShellInstance.Runspace = runspace;

                // load Powershell Script
                PowerShellInstance.AddScript(ScriptFilePath, true);

                // if script terminates service must terminate, too!
                PowerShellMonitor = (sender, e) =>
                {
                    if (e.InvocationStateInfo.State == PSInvocationState.Completed ||
                        e.InvocationStateInfo.State == PSInvocationState.Stopped ||
                        e.InvocationStateInfo.State == PSInvocationState.Failed)
                    {
                        WriteEntry(ServiceName,
                            string.Format("{0} script terminated with status '{1}'! - going to stop service!",
                                ServiceName, e.InvocationStateInfo.State), EventLogEntryType.Information);
                        // using no different thread here causes a deadlock!
                        Task.Run(() => { Stop(); });
                    }
                };
                PowerShellInstance.InvocationStateChanged += PowerShellMonitor;

                // start script
                AsyncPowerShell = PowerShellInstance.BeginInvoke();

                // Set the variable holding the exit code set by script to default (=0)
                ExitCodeByScript = 0;

                // Success. Set the service state to Running.
                ServiceStatus.dwCurrentState = ServiceRunning;
            }
            catch (Exception e)
            {
                WriteEntry(ServiceName, string.Format("{0} threw an exception: {1}", ServiceName, e),
                    EventLogEntryType.Error);

                if (PowerShellInstance != null)
                {
                    if (PowerShellInstance.Runspace != null)
                    {
                        PowerShellInstance.Runspace.Dispose();
                    }

                    PowerShellInstance.Dispose();
                    PowerShellInstance = null;
                }

                Power = null;
                SessionChange = null;
                Pause = null;
                Continue = null;
                Shutdown = null;
                StopEvent = null;
                CustomCommand = null;

                AsyncPowerShell = null;

                ServiceStatus.dwCurrentState = ServiceStopped;
                ServiceStatus.dwWin32ExitCode = (int) (Win32Error.ErrorExceptionInService);
            }
            finally
            {
                ServiceStatus.dwWaitHint = 0;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);
            }
        }

        protected override void OnStop()
        {
            try
            {
                // stop monitoring the PowerShell's state
                PowerShellInstance.InvocationStateChanged -= PowerShellMonitor;

                ServiceStatus.dwCurrentState = ServiceStopPending;
                ServiceStatus.dwWin32ExitCode = (int) Win32Error.NoError;
                ServiceStatus.dwWaitHint = ServiceStopTimeout;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);

                // fire event only if script is still running
                EventHandler<EventArgs> handler = StopEvent;
                if (handler != null && PowerShellInstance.InvocationStateInfo.State == PSInvocationState.Running)
                {
                    try
                    {
                        handler.Invoke(this, EventArgs.Empty);
                    }
                    catch (Exception e)
                    {
                        WriteEntry(ServiceName,
                            string.Format("{0}`s script`s event handler \"stop\" threw an exception: {1}", ServiceName,
                                e), EventLogEntryType.Error);
                        // do not rethrow
                    }
                }

                // if script doesn't terminate, kill Powershell!
                PSDataCollection<PSObject> output;
                using (WaitHandle powerShellWaitHandle = AsyncPowerShell.AsyncWaitHandle)
                {
                    if (powerShellWaitHandle.WaitOne(ScriptStopTimeout))
                    {
                        try
                        {
                            output = PowerShellInstance.EndInvoke(AsyncPowerShell);

                            if (output != null && output.Count > 0)
                            {
                                using (StringWriter stringWriter = new StringWriter())
                                {
                                    stringWriter.WriteLine($"Script returned {output.Count} objects:");
                                    foreach (PSObject psOutputElement in output)
                                    {
                                        stringWriter.WriteLine(psOutputElement);
                                    }

                                    stringWriter.Flush();
                                    EventLog.WriteEntry(stringWriter.ToString(), EventLogEntryType.Warning);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            WriteEntry(ServiceName, $"{ServiceName}`s script threw an exception: {e}",
                                EventLogEntryType.Error);
                            // do not rethrow
                        }
                    }
                    else
                    {
                        WriteEntry(ServiceName,
                            $"{ServiceName} script did not terminate on stop event - going to kill!",
                            EventLogEntryType.Warning);
                        PowerShellInstance.Stop();
                    }
                }

                ServiceStatus.dwCurrentState = ServiceStopped;
                if (ExitCodeByScript > 0)
                {
                    ServiceStatus.dwWin32ExitCode = (int) Win32Error.ErrorServiceSpecificError;
                    ServiceStatus.dwServiceSpecificExitCode = ExitCodeByScript;
                }
                else
                {
                    ServiceStatus.dwWin32ExitCode = (int) Win32Error.NoError;
                }
            }

            catch (Exception e)
            {
                WriteEntry(ServiceName, string.Format("{0} threw an exception: {1}", ServiceName, e),
                    EventLogEntryType.Error);
                ServiceStatus.dwWin32ExitCode = (int) (Win32Error.ErrorExceptionInService);
            }
            finally
            {
                // in an example of Microsoft, the PowerShell object is closed before the runspace so
                // we do it in the same order here
                if (PowerShellInstance != null)
                {
                    Runspace runspace = PowerShellInstance.Runspace;
                    PowerShellInstance.Dispose();
                    AsyncPowerShell = null;
                    PowerShellInstance = null;
                    if (runspace != null)
                    {
                        runspace.Dispose();
                    }
                }

                // clean up event registration
                // attention: due to this line don't use the events only in Powershell - not in
                // this .net class
                Power = null;
                SessionChange = null;
                Pause = null;
                Continue = null;
                Shutdown = null;
                StopEvent = null;
                CustomCommand = null;

                ServiceStatus.dwWaitHint = 0;
                SetServiceStatus(ServiceHandle, ref ServiceStatus);
            }
        }

        protected void InitFromAppSettings(bool reload)
        {
            ConfigurationManager.RefreshSection("appSettings");
            NameValueCollection applicationSettings = ConfigurationManager.AppSettings;
            HashSet<string> settingKeys = new HashSet<string>(applicationSettings.AllKeys);
            ServiceName = settingKeys.Contains(ServiceNameKeyConst)
                ? applicationSettings[ServiceNameKeyConst]
                : "PowerShell Service";
            LogName = settingKeys.Contains(LogNameKeyConst) ? applicationSettings[LogNameKeyConst] : "Application";
            bool parsedValue = false;
            int parsedIntValue = 0;
            ScriptCanHandlePowerEvent = (settingKeys.Contains(CanHandlePowerEventKeyConst) &&
                                         bool.TryParse(applicationSettings[CanHandlePowerEventKeyConst],
                                             out parsedValue)) && parsedValue;
            CanShutdown = (settingKeys.Contains(CanShutdownKeyConst) &&
                           bool.TryParse(applicationSettings[CanShutdownKeyConst], out parsedValue)) && parsedValue;
            CanStop = (!settingKeys.Contains(CanStopKeyConst) ||
                       !bool.TryParse(applicationSettings[CanStopKeyConst], out parsedValue)) || parsedValue;
            CanPauseAndContinue = (settingKeys.Contains(CanPauseAndContinueKeyConst) &&
                                   bool.TryParse(applicationSettings[CanPauseAndContinueKeyConst], out parsedValue)) &&
                                  parsedValue;
            CanHandleSessionChangeEvent = (settingKeys.Contains(CanHandleSessionChangeEventKeyConst) &&
                                           bool.TryParse(applicationSettings[CanHandleSessionChangeEventKeyConst],
                                               out parsedValue)) && parsedValue;
            AllowSuspend = (settingKeys.Contains(AllowSuspendKeyConst) &&
                            bool.TryParse(applicationSettings[AllowSuspendKeyConst], out parsedValue)) && parsedValue;
            AutoLog = (settingKeys.Contains(AutoLogKeyConst) &&
                       bool.TryParse(applicationSettings[AutoLogKeyConst], out parsedValue)) && parsedValue;
            ServiceStartTimeout = (settingKeys.Contains(ServiceStartTimeoutKeyConst) &&
                                   Int32.TryParse(applicationSettings[ServiceStartTimeoutKeyConst], out parsedIntValue))
                ? parsedIntValue
                : 30000;
            ServiceStopTimeout = (settingKeys.Contains(ServiceStopTimeoutKeyConst) &&
                                  Int32.TryParse(applicationSettings[ServiceStopTimeoutKeyConst], out parsedIntValue))
                ? parsedIntValue
                : 30000;
            ServicePauseTimeout = (settingKeys.Contains(ServicePauseTimeoutKeyConst) &&
                                   Int32.TryParse(applicationSettings[ServicePauseTimeoutKeyConst], out parsedIntValue))
                ? parsedIntValue
                : 30000;
            ServiceContinueTimeout = (settingKeys.Contains(ServiceContinueTimeoutKeyConst) &&
                                      Int32.TryParse(applicationSettings[ServiceContinueTimeoutKeyConst],
                                          out parsedIntValue))
                ? parsedIntValue
                : 30000;
            ScriptStopTimeout = (settingKeys.Contains(ScriptStopTimeoutKeyConst) &&
                                 Int32.TryParse(applicationSettings[ScriptStopTimeoutKeyConst], out parsedIntValue))
                ? parsedIntValue
                : 25000;
            ScriptFilePath = applicationSettings[ScriptFilePathKeyConst];
        }

        public static void Main()
        {
            Run(new PowershellService());
        }
    }
}