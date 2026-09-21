package com.phairplay.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.os.Binder
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.phairplay.MainActivity
import com.phairplay.R
import android.view.Surface
import com.phairplay.airplay.AirPlayReceiver
import com.phairplay.cast.CastReceiver
import com.phairplay.miracast.MiracastReceiver
import com.phairplay.settings.AppSettings
import com.phairplay.settings.SettingsRepository
import com.phairplay.util.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * PhairPlayService — Android ForegroundService that hosts all receiver protocols.
 *
 * WHY: The AirPlay/Miracast/Cast receivers need to run continuously in the background.
 * Android may kill background processes. A ForegroundService with a persistent
 * notification keeps the app alive and shows the user that PhairPlay is active.
 *
 * HOW: Bind to this service from [MainActivity] to receive state updates.
 * Use [ServiceController] to send start/stop/restart commands.
 *
 * Service lifecycle:
 *   startForegroundService() → onCreate() → onStartCommand() → [running in background]
 *   stopSelf() / stopService() → onDestroy() → all receivers stopped
 *
 * Commands via Intent actions (sent by [ServiceController]):
 *   ACTION_START   — starts all enabled receivers
 *   ACTION_STOP    — stops all receivers and stops the service
 *   ACTION_RESTART — stops then starts all receivers (service keeps running)
 */
class PhairPlayService : Service() {

    // Binder for Activity binding (returns this service directly)
    private val binder = LocalBinder()

    // Coroutine scope — cancelled in onDestroy() to clean up all coroutines
    private val serviceJob = SupervisorJob()
    private val serviceScope = CoroutineScope(Dispatchers.IO + serviceJob)
    private val receiverMutex = Mutex()

    // Network recovery is owned by the foreground service so it continues working while the
    // Activity is not open. A short debounce avoids tearing down the receiver during a normal
    // Wi-Fi handoff between access points.
    private val connectivityManager by lazy {
        getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    }
    private var networkCallback: ConnectivityManager.NetworkCallback? = null
    private var networkRecoveryJob: Job? = null
    @Volatile private var receiverRunRequested = false
    @Volatile private var waitingForNetwork = false

    // Observable state — Activities and Fragments observe this via the binder
    private val _serviceState = MutableStateFlow<ServiceState>(ServiceState.Stopped)
    val serviceState: StateFlow<ServiceState> = _serviceState.asStateFlow()

    private val _airPlayState = MutableStateFlow(ProtocolState.DISABLED)
    val airPlayState: StateFlow<ProtocolState> = _airPlayState.asStateFlow()

    private val _miracastState = MutableStateFlow(ProtocolState.DISABLED)
    val miracastState: StateFlow<ProtocolState> = _miracastState.asStateFlow()

    private val _castState = MutableStateFlow(ProtocolState.DISABLED)
    val castState: StateFlow<ProtocolState> = _castState.asStateFlow()

    private val _activeConnection = MutableStateFlow<ActiveConnection?>(null)
    val activeConnection: StateFlow<ActiveConnection?> = _activeConnection.asStateFlow()

    private val _photoFrame = MutableStateFlow<PhotoFrame?>(null)
    val photoFrame: StateFlow<PhotoFrame?> = _photoFrame.asStateFlow()

    // Non-null while AirPlay audio is playing WITHOUT video — drives the now-playing overlay.
    private val _nowPlaying = MutableStateFlow<com.phairplay.airplay.NowPlayingInfo?>(null)
    val nowPlaying: StateFlow<com.phairplay.airplay.NowPlayingInfo?> = _nowPlaying.asStateFlow()

    // Non-null while a PIN should be shown on screen for SRP pair-setup (PIN access control).
    private val _pairingPin = MutableStateFlow<String?>(null)
    val pairingPin: StateFlow<String?> = _pairingPin.asStateFlow()

    // Surface provider — supplied by MainActivity after binding (Sprint 5).
    // The lambda captures this field so it always uses the latest provider even if
    // setVideoSurfaceProvider() is called after startAirPlay().
    @Volatile private var videoSurfaceProvider: (() -> Surface?)? = null

    // Receiver instances — null when not running
    private var airPlayReceiver: AirPlayReceiver? = null
    private var miracastReceiver: MiracastReceiver? = null
    private var castReceiver: CastReceiver? = null

    // Settings — read once when starting, re-read on restart
    private lateinit var settingsRepository: SettingsRepository

    // ─── Service Lifecycle ───────────────────────────────────────────────────

    override fun onCreate() {
        super.onCreate()
        Logger.i("PhairPlayService created")
        settingsRepository = SettingsRepository(applicationContext)
        createNotificationChannel()
        registerNetworkMonitoring()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // Promote to foreground immediately with a persistent notification
        startForeground(NOTIFICATION_ID, buildNotification(isRunning = false))

        when (intent?.action) {
            ACTION_START   -> {
                receiverRunRequested = true
                serviceScope.launch {
                    receiverMutex.withLock { startReceivers() }
                }
            }
            ACTION_STOP    -> {
                receiverRunRequested = false
                waitingForNetwork = false
                networkRecoveryJob?.cancel()
                serviceScope.launch {
                    receiverMutex.withLock { stopReceivers() }
                    stopSelf()
                }
            }
            ACTION_RESTART -> {
                receiverRunRequested = true
                serviceScope.launch {
                    receiverMutex.withLock { restartReceivers() }
                }
            }
            else           -> {
                // START_STICKY restarts arrive with a null intent. Recreate the receivers so the
                // device is available again without requiring the user to open the UI.
                receiverRunRequested = true
                serviceScope.launch {
                    receiverMutex.withLock { startReceivers() }
                }
            }
        }

        // START_STICKY: if the system kills the service, restart it with a null intent
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder = binder

    /**
     * The Activity task was removed from recents. Keep the foreground receiver alive so the TV
     * remains discoverable and can accept a new share without reopening the app.
     */
    override fun onTaskRemoved(rootIntent: Intent?) {
        // Keep the foreground receiver alive when the launcher removes the Activity task. The
        // persistent notification and explicit Stop action are the user-visible lifecycle controls.
        Logger.i("App task removed — keeping PhairPlayService alive")
        super.onTaskRemoved(rootIntent)
    }

    /**
     * Called by [MainActivity] after it binds, to supply the [Surface] for video rendering.
     *
     * The lambda is invoked lazily — only when a stream is actually being started — so it
     * is safe to call this before or after [startAirPlay]. The lambda should return null
     * if the Activity's StreamingScreen is not yet available (e.g., surface not yet created).
     *
     * Call with `{ null }` (or simply don't call) during Activity destruction so we stop
     * holding a reference to the Activity's Surface after the window is gone.
     *
     * @param provider Lambda that returns the current [Surface], or null if unavailable.
     */
    fun setVideoSurfaceProvider(provider: () -> Surface?) {
        videoSurfaceProvider = provider
    }

    /**
     * Sends a DACP transport command (TV remote → AirPlay sender), e.g. play/pause or skip what the
     * Mac/iPhone is streaming. Bound Activities call this from media-key events. No-op if no AirPlay
     * sender has advertised a DACP identity.
     */
    fun sendAirPlayRemoteCommand(command: String) {
        airPlayReceiver?.sendRemoteCommand(command)
    }

    override fun onDestroy() {
        Logger.i("PhairPlayService destroying")
        networkRecoveryJob?.cancel()
        unregisterNetworkMonitoring()
        stopAllReceiversInternal()
        serviceJob.cancel()
        super.onDestroy()
    }

    // ─── Service Control ─────────────────────────────────────────────────────

    /**
     * Starts all receivers that are enabled in Settings.
     *
     * Reads current settings, then starts AirPlay, Miracast, and/or Cast
     * receivers according to the enabled flags.
     */
    private suspend fun startReceivers() {
        if (!hasUsableNetwork()) {
            waitingForNetwork = true
            _serviceState.value = ServiceState.Running
            updateNotification(
                isRunning = true,
                notificationContentText = getString(R.string.notification_status_waiting_network)
            )
            Logger.w("No Wi-Fi/Ethernet network available — receivers will start when network returns")
            return
        }

        waitingForNetwork = false
        val settings = settingsRepository.settingsFlow.first()
        Logger.i("Starting receivers: AirPlay=${settings.airPlayEnabled}, Miracast=${settings.miracastEnabled}, Cast=${settings.castEnabled}")

        _serviceState.value = ServiceState.Running
        updateNotification(isRunning = true)

        if (settings.airPlayEnabled)   startAirPlay(settings)
        if (settings.miracastEnabled)  startMiracast()
        if (settings.castEnabled)      startCast()
    }

    /**
     * Stops all active receivers and updates the service state to Stopped.
     * Does NOT call stopSelf() — use [ACTION_STOP] for that.
     */
    private fun stopReceivers() {
        Logger.i("Stopping all receivers")
        stopAllReceiversInternal()
        waitingForNetwork = false
        _serviceState.value = ServiceState.Stopped
        _activeConnection.value = null
        updateNotification(isRunning = false)
    }

    /**
     * Restarts all receivers: stops them, waits briefly, then starts them again.
     * Used for applying settings changes or recovering from errors.
     */
    private suspend fun restartReceivers() {
        Logger.i("Restarting all receivers")
        _serviceState.value = ServiceState.Restarting
        updateNotification(isRunning = false)
        stopAllReceiversInternal()
        kotlinx.coroutines.delay(500) // brief pause to ensure ports are released
        startReceivers()
    }

    // ─── Network recovery ───────────────────────────────────────────────────

    /**
     * Watches Wi-Fi/Ethernet independently of the Activity. On a real network loss we close the
     * current RTSP/P2P sockets and media decoders; when a usable network returns, fresh mDNS and
     * listening sockets are created. A sender may still need to retry AirPlay, but the receiver is
     * ready and visible again without reopening the app.
     */
    private fun registerNetworkMonitoring() {
        val request = NetworkRequest.Builder()
            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
            .addTransportType(NetworkCapabilities.TRANSPORT_ETHERNET)
            .build()
        val callback = object : ConnectivityManager.NetworkCallback() {
            override fun onLost(network: Network) {
                if (!receiverRunRequested) return
                Logger.w("Network lost: $network — scheduling receiver recovery")
                waitingForNetwork = true
                scheduleNetworkReconcile()
            }

            override fun onAvailable(network: Network) {
                if (!receiverRunRequested) return
                Logger.i("Network available: $network — scheduling receiver recovery")
                scheduleNetworkReconcile()
            }
        }
        try {
            connectivityManager.registerNetworkCallback(request, callback)
            networkCallback = callback
        } catch (e: Exception) {
            // The receiver still works on devices with unusual/OEM connectivity managers; this
            // only disables automatic recovery and leaves the explicit Restart action available.
            Logger.e("Unable to register network recovery callback", e)
        }
    }

    private fun unregisterNetworkMonitoring() {
        networkCallback?.let { callback ->
            runCatching { connectivityManager.unregisterNetworkCallback(callback) }
        }
        networkCallback = null
    }

    private fun scheduleNetworkReconcile() {
        networkRecoveryJob?.cancel()
        networkRecoveryJob = serviceScope.launch {
            delay(NETWORK_RECONCILE_DELAY_MS)
            if (!receiverRunRequested) return@launch

            receiverMutex.withLock {
                if (!hasUsableNetwork()) {
                    if (hasAnyReceiver()) {
                        Logger.w("Network is still unavailable — stopping receivers until it returns")
                        stopAllReceiversInternal()
                    }
                    waitingForNetwork = true
                    updateNotification(
                        isRunning = true,
                        notificationContentText = getString(R.string.notification_status_waiting_network)
                    )
                    return@withLock
                }

                // A lost network invalidates the old mDNS registrations and sockets even when the
                // OS reports a new network before the debounce expires. Recreate all receivers so
                // the new interface/IP is advertised correctly.
                if (waitingForNetwork) {
                    Logger.i("Network restored — restarting receivers and mDNS")
                    stopAllReceiversInternal()
                    delay(NETWORK_RESTART_DELAY_MS)
                    startReceivers()
                }
            }
        }
    }

    private fun hasAnyReceiver(): Boolean =
        airPlayReceiver != null || miracastReceiver != null || castReceiver != null

    /** Returns true for a usable local Wi-Fi or Ethernet link; internet validation is not required. */
    private fun hasUsableNetwork(): Boolean = connectivityManager.allNetworks.any { network ->
        val capabilities = connectivityManager.getNetworkCapabilities(network) ?: return@any false
        capabilities.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) ||
            capabilities.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET)
    }

    // ─── Individual Protocol Starters ────────────────────────────────────────

    /**
     * Creates and starts the [AirPlayReceiver].
     *
     * The display name comes from settings — blank means use the Android device name,
     * which [MdnsService] resolves at runtime.
     *
     * Surface is not available here (it lives in the Activity/Fragment).
     * The surface provider is wired up from [MainActivity] in Sprint 5.
     * Until then, video frames are silently discarded and only audio plays.
     *
     * @param settings Current app settings; read once per start/restart cycle.
     */
    private fun startAirPlay(settings: AppSettings) {
        // Mirror the debug-overlay setting into the shared stats bus that StreamingScreen reads.
        com.phairplay.airplay.StreamStats.overlayEnabled = settings.showDebugOverlay

        // Idempotent: a redundant ACTION_START (e.g. the activity being recreated while the
        // foreground service is still alive) must NOT spin up a second AirPlayReceiver competing
        // for port 7000. The existing receiver keeps running and picks up the new Surface via the
        // surfaceProvider. A genuine restart goes through ACTION_RESTART (stop → delay → start).
        if (airPlayReceiver != null) {
            Logger.i("AirPlay receiver already running — skipping duplicate start")
            return
        }
        // Captures the sender name reported by AirPlayReceiver before CONNECTED fires.
        // onSenderNameChanged is called synchronously before emitState(CONNECTED), so
        // this assignment happens-before the Main-thread read in onStateChanged.
        var pendingSenderName = "AirPlay Sender"

        airPlayReceiver = AirPlayReceiver(
            context = applicationContext,
            displayName = settings.effectiveDisplayName,
            mirrorWidth = settings.mirrorWidth,
            mirrorHeight = settings.mirrorHeight,
            audioEnabled = settings.mirrorAudioEnabled,
            pinAuthEnabled = settings.airPlayPinAuthEnabled,
            // Delegate to the current provider at call time — captures the field, not a fixed value.
            // When MainActivity calls setVideoSurfaceProvider(), future surface requests use it.
            videoSurfaceProvider = { videoSurfaceProvider?.invoke() },
            onSenderNameChanged = { name ->
                pendingSenderName = name.ifEmpty { "AirPlay Sender" }
            },
            onPhotoReceived = { bytes, imageType ->
                _photoFrame.value = PhotoFrame(
                    bytes = bytes.copyOf(),
                    mimeType = imageType.mimeType
                )
                updateNotification(isRunning = true)
            },
            onPhotoCleared = {
                _photoFrame.value = null
            },
            onNowPlayingChanged = { info ->
                _nowPlaying.value = info
            },
            onPinChanged = { pin ->
                _pairingPin.value = pin
            },
            onStateChanged = { state ->
                _airPlayState.value = state
                when (state) {
                    ProtocolState.CONNECTED   -> {
                        _photoFrame.value = null
                        _activeConnection.value =
                            ActiveConnection(pendingSenderName, Protocol.AIRPLAY)
                        updateNotification(isRunning = true, streamingSenderName = pendingSenderName)
                        // When the receiver was started on boot, no Activity may exist yet. Bring
                        // the TV streaming surface forward as soon as a sender connects so video
                        // has a real Surface to render into without requiring a manual app launch.
                        bringStreamingActivityToFront()
                    }
                    ProtocolState.ADVERTISING,
                    ProtocolState.DISABLED,
                    ProtocolState.ERROR       -> {
                        _activeConnection.value = null
                        updateNotification(isRunning = state != ProtocolState.DISABLED &&
                                                       state != ProtocolState.ERROR)
                    }
                }
            }
        ).also { it.start() }
        Logger.d("AirPlay receiver started (displayName='${settings.effectiveDisplayName}')")
    }

    private fun startMiracast() {
        _miracastState.value = ProtocolState.ADVERTISING
        miracastReceiver = MiracastReceiver(
            context = applicationContext,
            onStateChanged = { state -> _miracastState.value = state }
        ).also { it.start() }
        Logger.d("Miracast receiver started")
    }

    private fun startCast() {
        _castState.value = ProtocolState.ADVERTISING
        castReceiver = CastReceiver(
            context = applicationContext,
            onStateChanged = { state -> _castState.value = state }
        ).also { it.start() }
        Logger.d("Cast receiver started")
    }

    private fun stopAllReceiversInternal() {
        try { airPlayReceiver?.stop() } catch (e: Exception) { Logger.e("AirPlay stop error", e) }
        try { miracastReceiver?.stop() } catch (e: Exception) { Logger.e("Miracast stop error", e) }
        try { castReceiver?.stop() } catch (e: Exception) { Logger.e("Cast stop error", e) }
        airPlayReceiver = null
        miracastReceiver = null
        castReceiver = null
        _airPlayState.value = ProtocolState.DISABLED
        _miracastState.value = ProtocolState.DISABLED
        _castState.value = ProtocolState.DISABLED
        _photoFrame.value = null
        _nowPlaying.value = null
        _pairingPin.value = null
    }

    // ─── Notification ────────────────────────────────────────────────────────

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                getString(R.string.notification_channel_name),
                NotificationManager.IMPORTANCE_LOW  // LOW: no sound, minimal visual interruption
            ).apply {
                description = getString(R.string.notification_channel_description)
            }
            val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            manager.createNotificationChannel(channel)
        }
    }

    /**
     * Builds the persistent notification for the ForegroundService.
     *
     * The notification shows the service status and provides quick actions
     * so users can Stop or Restart without opening the app.
     *
     * @param isRunning            True if receivers are active; false if stopped/restarting.
     * @param notificationContentText Override for the notification body text.
     *   When null, the default running/stopped status string is used.
     *   Pass the sender name here (e.g. "Streaming from MacBook Pro") when connected.
     */
    private fun buildNotification(
        isRunning: Boolean,
        notificationContentText: String? = null
    ): Notification {
        // Tapping the notification opens the app
        val openAppIntent = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        // "Stop" action — sends ACTION_STOP to this service
        val stopIntent = PendingIntent.getService(
            this, 1,
            Intent(this, PhairPlayService::class.java).apply { action = ACTION_STOP },
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        // "Restart" action — sends ACTION_RESTART to this service
        val restartIntent = PendingIntent.getService(
            this, 2,
            Intent(this, PhairPlayService::class.java).apply { action = ACTION_RESTART },
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val statusText = if (isRunning) R.string.notification_status_running
                         else           R.string.notification_status_stopped

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(getString(R.string.app_name))
            .setContentText(notificationContentText ?: getString(statusText))
            .setContentIntent(openAppIntent)
            .setOngoing(true)                   // Prevents user from swiping away
            .setCategory(Notification.CATEGORY_SERVICE)
            .addAction(R.drawable.ic_stop,    getString(R.string.action_stop),    stopIntent)
            .addAction(R.drawable.ic_restart, getString(R.string.action_restart), restartIntent)
            .build()
    }

    private fun updateNotification(
        isRunning: Boolean,
        streamingSenderName: String? = null,
        notificationContentText: String? = null
    ) {
        val contentText = notificationContentText ?: streamingSenderName?.let {
            getString(R.string.notification_status_streaming, it)
        }
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.notify(NOTIFICATION_ID, buildNotification(isRunning, contentText))
    }

    /** Opens/reuses the TV Activity when a sender connects while the UI was not open. */
    private fun bringStreamingActivityToFront() {
        serviceScope.launch(Dispatchers.Main.immediate) {
            try {
                startActivity(Intent(this@PhairPlayService, MainActivity::class.java).apply {
                    addFlags(
                        Intent.FLAG_ACTIVITY_NEW_TASK or
                            Intent.FLAG_ACTIVITY_SINGLE_TOP or
                            Intent.FLAG_ACTIVITY_CLEAR_TOP
                    )
                })
            } catch (e: Exception) {
                // Some OEM launchers block background Activity starts. The receiver remains alive;
                // opening the persistent notification will still provide the rendering surface.
                Logger.w("Could not bring streaming Activity to front: ${e.message}")
            }
        }
    }

    // ─── Binder ─────────────────────────────────────────────────────────────

    /**
     * LocalBinder — Provides direct access to [PhairPlayService] for bound Activities.
     *
     * WHY: Binding (rather than just starting) the service gives the Activity a
     * direct reference, so it can observe the service's StateFlows without
     * using broadcasts or a shared ViewModel.
     */
    inner class LocalBinder : Binder() {
        fun getService(): PhairPlayService = this@PhairPlayService
    }

    companion object {
        const val CHANNEL_ID      = "phairplay_service_channel"
        const val NOTIFICATION_ID = 1001
        const val ACTION_START    = "com.phairplay.action.START"
        const val ACTION_STOP     = "com.phairplay.action.STOP"
        const val ACTION_RESTART  = "com.phairplay.action.RESTART"
        private const val NETWORK_RECONCILE_DELAY_MS = 1_000L
        private const val NETWORK_RESTART_DELAY_MS = 500L
    }
}

/**
 * PhotoFrame — latest still image received via AirPlay `/photo`.
 *
 * The bytes are kept in memory only and cleared on DELETE `/photo`, streaming
 * start, receiver stop, or service destruction.
 */
data class PhotoFrame(
    val bytes: ByteArray,
    val mimeType: String,
    val receivedAtMillis: Long = System.currentTimeMillis()
)
