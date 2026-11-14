import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/limiter'
const columns = [
  "规则编号",
  "限流对象",
  "限流算法",
  "限流阈值",
  "每秒上限",
  "突发容量",
  "判定口径",
  "降级动作",
  "恢复时间",
  "规则状态",
]
const actions = ["启用限流", "触发降级", "关闭限流"]
const statuses = ["草稿", "生效中", "已触发", "已关闭"]
const basisOptions = ["令牌桶", "固定窗口", "滑动窗口"]
const fieldHints: Record<string, string> = {
  规则编号: "如 LIMI-0002",
  限流对象: "被限流的服务名称",
  限流算法: "如 令牌桶",
  限流阈值: "窗口累计上限，须 ≥ 每秒上限",
  每秒上限: "每秒允许放行次数",
  突发容量: "突发额度上限，须 ≥ 限流阈值",
  判定口径: "令牌桶 / 固定窗口 / 滑动窗口",
  降级动作: "如 切换降级副本",
  恢复时间: "秒数",
  关联路由: "路由规则编号，用于降级同步",
}
const requiredFormFields = ["规则编号", "限流对象", "限流算法", "每秒上限", "突发容量", "降级动作", "恢复时间"]

type Row = Record<string, string | number | null> & { id: number }
type Receipt = Record<string, string | number | null>

type FormState = Record<string, string>

const emptyForm: FormState = {
  规则编号: "",
  限流对象: "",
  限流算法: "令牌桶",
  限流阈值: "10",
  每秒上限: "10",
  突发容量: "20",
  判定口径: "令牌桶",
  降级动作: "切换降级服务",
  恢复时间: "60",
  关联路由: "",
}

export default function LimiterView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [form, setForm] = useState<FormState>(emptyForm)
  const [editingId, setEditingId] = useState<number | null>(null)
  const [showForm, setShowForm] = useState(false)
  const [receipts, setReceipts] = useState<Receipt[]>([])
  const [receiptRule, setReceiptRule] = useState('')
  const filterFields = useMemo(() => ["规则编号", "限流对象", "规则状态"], [])

  const stats = useMemo(() => [
    { label: "限流规则总数", value: total },
    { label: "生效中限流", value: rows.filter((row) => row.status === '生效中').length },
    { label: "已触发限流", value: rows.filter((row) => row.status === '已触发').length },
  ], [rows, total])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('限流规则列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '调用限流降级列表读取失败')
    }
  }, [filters])

  const reloadReceipts = useCallback(async (ruleCode: string) => {
    setReceiptRule(ruleCode)
    if (!ruleCode) {
      setReceipts([])
      return
    }
    try {
      const suffix = ruleCode === '*' ? '' : `?rule_code=${encodeURIComponent(ruleCode)}`
      const response = await request(`${ENDPOINT}/receipts${suffix}`)
      if (!response.ok) {
        throw new Error('降级回执读取失败')
      }
      const payload = await response.json()
      setReceipts(payload.items ?? [])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '降级回执读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  function startCreate() {
    setEditingId(null)
    setForm(emptyForm)
    setShowForm(true)
  }

  function startEdit(row: Row) {
    setEditingId(row.id)
    setForm({
      规则编号: String(row.规则编号 ?? ''),
      限流对象: String(row.限流对象 ?? ''),
      限流算法: String(row.限流算法 ?? ''),
      限流阈值: String(row.限流阈值 ?? ''),
      每秒上限: String(row.每秒上限 ?? ''),
      突发容量: String(row.突发容量 ?? ''),
      判定口径: String(row.判定口径 ?? ''),
      降级动作: String(row.降级动作 ?? ''),
      恢复时间: String(row.恢复时间 ?? ''),
      关联路由: String(row.关联路由 ?? ''),
    })
    setShowForm(true)
  }

  async function save() {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(editingId ? `${ENDPOINT}/${editingId}` : ENDPOINT, {
        method: editingId ? 'PUT' : 'POST',
        body: JSON.stringify({ values: form }),
      })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message ?? '限流规则未保存')
      }
      setNotice(payload.message ?? '限流规则已保存')
      setShowForm(false)
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '限流规则保存失败')
    }
  }

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message ?? '调用限流降级动作未生效')
      }
      setNotice(payload.message)
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '调用限流降级操作失败')
    }
  }

  async function runCheck(row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/checks`, {
        method: 'POST',
        body: JSON.stringify({ values: { 规则编号: row.规则编号 } }),
      })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message ?? '限流判定失败')
      }
      const receipt = payload.receipt as Receipt | null
      setNotice(receipt
        ? `判定：${payload.entry?.判定结果}；回执 ${receipt.回执编号}（触发阈值：${receipt.触发阈值} ${receipt.触发阈值值}）`
        : `判定：${payload.entry?.判定结果}；${payload.message}`)
      await reload()
      await reloadReceipts(String(row.规则编号))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '限流判定失败')
    }
  }

  async function runRestore(row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/restore`, {
        method: 'POST',
        body: JSON.stringify({ values: { 规则编号: row.规则编号 } }),
      })
      const payload = await response.json()
      if (!response.ok || !payload.ok) {
        throw new Error(payload.message ?? '恢复失败')
      }
      setNotice(payload.message)
      await reload()
      await reloadReceipts(String(row.规则编号))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '恢复失败')
    }
  }

  return (
    <section className="page" data-module="limiter">
      <header className="page-head">
        <div>
          <h2>调用限流降级管理</h2>
          <p className="page-desc">维护限流阈值、每秒上限、突发容量与判定口径，按限流对象做判定并生成降级回执，恢复时间到达后从新窗口重新计数。阈值须满足：每秒上限 ≤ 限流阈值 ≤ 突发容量。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={startCreate}>登记限流规则</button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出调用限流降级清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      {showForm && (
        <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void save() }}>
          {Object.keys(emptyForm).map((field) => {
            const isBasis = field === '判定口径'
            return (
              <label key={field} className="filter-item">
                <span>{field}{requiredFormFields.includes(field) ? '（必填）' : '（可选）'}</span>
                {isBasis ? (
                  <select value={form[field]} onChange={(event) => setForm((prev) => ({ ...prev, [field]: event.target.value }))}>
                    {basisOptions.map((option) => <option key={option} value={option}>{option}</option>)}
                  </select>
                ) : (
                  <input
                    value={form[field]}
                    placeholder={fieldHints[field] ?? `填写${field}`}
                    onChange={(event) => setForm((prev) => ({ ...prev, [field]: event.target.value }))}
                  />
                )}
              </label>
            )
          })}
          <button className="btn primary" type="submit">{editingId ? '保存修改' : '提交登记'}</button>
          <button className="btn ghost" type="button" onClick={() => setShowForm(false)}>取消</button>
        </form>
      )}

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            {field === '规则状态' ? (
              <select value={filters[field] ?? ''} onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}>
                <option value="">全部</option>
                {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
              </select>
            ) : (
              <input
                value={filters[field] ?? ''}
                placeholder={`按${field}检索`}
                onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
              />
            )}
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
        <button className="btn ghost" type="button" onClick={() => void reloadReceipts('*')}>查看全部回执</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>当前窗口</th>
            <th>窗口状态</th>
            <th>判定 / 动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td>{row.当前窗口 ? `第 ${row.当前窗口} 窗` : '—'}</td>
              <td>{row.窗口状态 ?? '—'}</td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void runCheck(row)}>执行判定</button>
                <button className="link" type="button" onClick={() => void runRestore(row)}>恢复计数</button>
                <button className="link" type="button" onClick={() => void reloadReceipts(String(row.规则编号))}>查看回执</button>
                <button className="link" type="button" onClick={() => startEdit(row)}>修改阈值</button>
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 3}>暂无调用限流降级数据，可先登记限流规则</td>
            </tr>
          )}
        </tbody>
      </table>

      {receiptRule && (
        <section className="receipt-panel">
          <h3>降级回执（{receiptRule === '*' ? '全部规则' : receiptRule}）</h3>
          <table className="data-table">
            <thead>
              <tr>
                {["回执编号", "规则编号", "限流对象", "窗口序号", "触发阈值", "触发阈值值", "限流阈值", "每秒上限", "突发容量", "判定口径", "窗口内已通过", "触发时间", "恢复截止", "状态", "路由同步", "切换目标"].map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {receipts.map((receipt) => (
                <tr key={String(receipt.id)}>
                  {["回执编号", "规则编号", "限流对象", "窗口序号", "触发阈值", "触发阈值值", "限流阈值", "每秒上限", "突发容量", "判定口径", "窗口内已通过", "触发时间", "恢复截止", "状态", "路由同步", "切换目标"].map((column) => (
                    <td key={column}>{receipt[column] ?? '—'}</td>
                  ))}
                </tr>
              ))}
              {receipts.length === 0 && (
                <tr>
                  <td className="empty-state" colSpan={16}>暂无降级回执，触发阈值后自动生成</td>
                </tr>
              )}
            </tbody>
          </table>
        </section>
      )}

      <footer className="page-foot">
        <span>共 {total} 条调用限流降级记录</span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
