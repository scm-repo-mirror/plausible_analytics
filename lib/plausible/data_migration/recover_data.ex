defmodule Plausible.DataMigration.RecoverData do
  def run(opts \\ []) do
    src_ch =
      opts
      |> Keyword.fetch!(:source_url)
      |> connect()

    dst_ch =
      opts
      |> Keyword.fetch!(:destination_url)
      |> connect()

    with_conn(src_ch, fn src_conn ->
      with_conn(dst_ch, fn dst_conn ->
        do_run(src_conn, dst_conn, :events, opts)
        # do_run(src_conn, dst_conn, :sessions, opts)
      end)
    end)
  end

  @byte_threshold 100_000

  defp do_run(src_conn, dst_conn, table, opts) do
    chunk_fun = fn data, acc ->
      buffer = [acc.buffer | data]
      size = acc.size + IO.iodata_length(data)

      if size > @byte_threshold do
        {:cont, buffer, %{buffer: [], size: 0}}
      else
        {:cont, %{buffer: buffer, size: size}}
      end
    end

    after_fun = fn
      %{buffer: []} -> {:cont, %{buffer: [], size: 0}}
      %{buffer: buffer} -> {:cont, buffer, %{buffer: [], size: 0}}
    end

    {select_sql, select_params} = select_sql(table, opts)
    insert_sql = insert_sql(table)

    src_conn
    |> Ch.stream(select_sql, select_params)
    |> Stream.map(fn %Ch.Result{data: data} -> data end)
    |> Stream.chunk_while(%{buffer: [], size: 0}, chunk_fun, after_fun)
    |> Stream.into(Ch.stream(dst_conn, insert_sql, %{}))
    |> Stream.run()
  end

  defp with_conn(ch, fun) do
    DBConnection.run(ch, fun, timeout: :infinity)
  end

  defp connect(url) do
    %{
      userinfo: userinfo,
      host: hostname,
      scheme: scheme,
      port: port,
      path: "/" <> database
    } = URI.parse(url)

    {username, password} =
      case userinfo && String.split(userinfo, ":") do
        nil -> {nil, nil}
        [username] when byte_size(username) > 0 -> {username, nil}
        [username, password] -> {username, password}
      end

    {:ok, ch} =
      Ch.start_link(
        scheme: to_string(scheme),
        hostname: hostname,
        port: port,
        database: database,
        username: username,
        password: password,
        timeout: :infinity,
        pool_size: 1
      )

    ch
  end

  defp select_sql(table, opts) do
    recovery_id = Keyword.fetch!(opts, :recovery_id)

    {select_sql(table), [recovery_id]}
  end

  defp select_sql(:events) do
    """
    SELECT
    `timestamp`,
    `name`,
    `site_id`,
    `user_id`,
    `session_id`,
    `hostname`,
    `pathname`,
    `referrer`,
    `referrer_source`,
    `country_code`,
    `screen_size`,
    `operating_system`,
    `browser`,
    `utm_medium`,
    `utm_source`,
    `utm_campaign`,
    `meta.key`,
    `meta.value`,
    `browser_version`,
    `operating_system_version`,
    `subdivision1_code`,
    `subdivision2_code`,
    `city_geoname_id`,
    `utm_content`,
    `utm_term`,
    `revenue_reporting_amount`,
    `revenue_reporting_currency`,
    `revenue_source_amount`,
    `revenue_source_currency`,
    `channel`,
    `click_id_param`,
    `scroll_depth`,
    `engagement_time`,
    {$0:UInt64} AS recovery_id
    FROM plausible_events_db.events_v2 
    WHERE site_id = 2 AND 
    timestamp >= '2025-11-05 23:35:56' AND 
    timestamp <= '2025-11-06 06:57:11'
    FORMAT CSV
    """
  end

  defp select_sql(:sessions) do
    """
    SELECT
    *,
    {$0:UInt64} as recovery_id
    FROM plausible_persistor.sessions_v2
    WHERE site_id = 2 AND 
    timestamp >= '2025-11-05 23:35:56' AND 
    timestamp <= '2025-11-06 06:57:11'
    """
  end

  defp insert_sql(:events) do
    """
    INSERT INTO plausible_events_db.events_v2
    (
      `timestamp`,
      `name`,
      `site_id`,
      `user_id`,
      `session_id`,
      `hostname`,
      `pathname`,
      `referrer`,
      `referrer_source`,
      `country_code`,
      `screen_size`,
      `operating_system`,
      `browser`,
      `utm_medium`,
      `utm_source`,
      `utm_campaign`,
      `meta.key`,
      `meta.value`,
      `browser_version`,
      `operating_system_version`,
      `subdivision1_code`,
      `subdivision2_code`,
      `city_geoname_id`,
      `utm_content`,
      `utm_term`,
      `revenue_reporting_amount`,
      `revenue_reporting_currency`,
      `revenue_source_amount`,
      `revenue_source_currency`,
      `channel`,
      `click_id_param`,
      `scroll_depth`,
      `engagement_time`,
      `recovery_id`
    ) FORMAT CSV
    """
  end

  defp insert_sql(:sessions) do
    "SELECT 1"
  end
end
