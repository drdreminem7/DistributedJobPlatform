read_field <- function(source, name) {
  match <- regmatches(source, regexpr(paste0('"', name, '":[0-9.]+'), source))
  as.numeric(sub(paste0('"', name, '":'), '', match))
}

read_report <- function(path) {
  source <- readLines(path, warn = FALSE)
  claim <- regmatches(source, regexpr('"claim_latency":\\{[^}]+\\}', source))
  c(throughput = read_field(source, 'jobs_per_second'),
    claim_p95 = read_field(claim, 'p95_ms'))
}

baseline <- read_report('benchmarks/results/djp-v7-baseline.json')
optimized <- read_report('benchmarks/results/djp-v7-optimized.json')
colors <- c('#8193a7', '#167b64')

pdf('benchmarks/results/performance.pdf', width = 10, height = 4.5)
par(mfrow = c(1, 2), mar = c(5, 4.5, 4, 1), bg = 'white')

throughput <- c(baseline['throughput'], optimized['throughput'])
positions <- barplot(throughput, names.arg = c('Original', 'Indexed'), col = colors,
                     border = NA, ylim = c(0, 3400), ylab = 'Completed jobs / second',
                     main = 'Completion throughput')
text(positions, throughput + 130, labels = format(round(throughput), big.mark = ','), cex = 1.1)

claim <- c(baseline['claim_p95'], optimized['claim_p95'])
positions <- barplot(claim, names.arg = c('Original', 'Indexed'), col = colors,
                     border = NA, ylim = c(0, 10), ylab = 'Milliseconds',
                     main = 'Successful claim p95')
text(positions, claim + 0.35, labels = sprintf('%.2f ms', claim), cex = 1.1)

dev.off()
