package tv.own.owntv

import android.content.Context
import android.util.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.koin.core.context.GlobalContext
import tv.own.owntv.core.database.entity.SourceEntity
import tv.own.owntv.core.model.SourceType
import tv.own.owntv.core.parser.XtreamM3uLink
import tv.own.owntv.core.repository.SourceRepository
import tv.own.owntv.core.settings.SettingsRepository
import tv.own.owntv.core.sync.SyncContentTypes
import tv.own.owntv.core.sync.work.CatalogSyncScheduler
import java.util.concurrent.TimeUnit

/**
 * Fork addition (based on OwnTV by ahXN00, GPL-3.0): at startup, reads one Xtream "get.php" line from
 * a text file on the local network and creates or updates the "Maison" source from it.
 * Any failure is swallowed: the app then starts with its last known playlist.
 */
object HomePlaylistSync {
    private const val TAG = "HomePlaylistSync"
    private const val PLAYLIST_URL = "http://192.168.1.111/playlist.txt"
    private const val SOURCE_NAME = "Maison"
    private const val PREFS = "home_playlist_sync"
    private const val KEY_LINE = "last_line"
    private const val KEY_SOURCE_ID = "source_id"
    private const val MAX_BYTES = 64L * 1024

    suspend fun run(context: Context) {
        try {
            val line = fetchFirstLine() ?: return
            apply(context.applicationContext, line)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Log.w(TAG, "ignored: ${e.javaClass.simpleName}")
        }
    }

    private suspend fun fetchFirstLine(): String? = withContext(Dispatchers.IO) {
        val client = OkHttpClient.Builder()
            .connectTimeout(3, TimeUnit.SECONDS)
            .readTimeout(3, TimeUnit.SECONDS)
            .callTimeout(3, TimeUnit.SECONDS)
            .retryOnConnectionFailure(false)
            .build()
        val request = Request.Builder().url(PLAYLIST_URL).build()
        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                Log.i(TAG, "HTTP ${response.code}")
                return@use null
            }
            val body = response.body ?: return@use null
            if (body.contentLength() > MAX_BYTES) return@use null
            body.string()
                .removePrefix("\uFEFF")
                .lineSequence()
                .map { it.trim().removePrefix("\uFEFF") }
                .firstOrNull { it.isNotEmpty() && !it.startsWith("#") }
        }
    }

    private suspend fun apply(context: Context, line: String) {
        val link = XtreamM3uLink.parse(line)
        if (link == null) {
            Log.i(TAG, "invalid line, nothing done")
            return
        }
        val koin = GlobalContext.get()
        val settings = koin.get<SettingsRepository>()
        val repo = koin.get<SourceRepository>()
        val scheduler = koin.get<CatalogSyncScheduler>()

        val profileId = settings.activeProfileId.first()
        if (profileId < 1L) {
            Log.i(TAG, "no active profile yet, nothing done")
            return
        }

        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        val knownId = prefs.getLong(KEY_SOURCE_ID, -1L)
        val known = if (knownId > 0) repo.getById(knownId) else null
        if (known != null && prefs.getString(KEY_LINE, null) == line) {
            Log.i(TAG, "line unchanged, nothing done")
            return
        }

        val target = known ?: repo.observeSources(profileId).first()
            .firstOrNull { it.type == SourceType.XTREAM && it.name == SOURCE_NAME }

        val saved: SourceEntity = if (target == null) {
            repo.addXtreamSource(
                profileId = profileId,
                name = SOURCE_NAME,
                serverUrl = link.server,
                username = link.username,
                password = link.password,
                preferHls = link.preferHls,
            )
        } else {
            val updated = target.copy(
                url = link.server,
                username = link.username,
                password = link.password,
                preferHls = link.preferHls,
            )
            repo.updateSource(updated)
            updated
        }
        prefs.edit().putLong(KEY_SOURCE_ID, saved.id).apply()

        scheduler.enqueueSync(
            saved.id,
            reason = "home_playlist",
            contentTypes = SyncContentTypes.enabledOf(saved),
        )
        prefs.edit().putString(KEY_LINE, line).apply()
        Log.i(TAG, "source ${if (target == null) "created" else "updated"}, sync enqueued")
    }
}