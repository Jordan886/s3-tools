const utilities = {
  checkDuplicates: async function checkDuplicates(array) {
    // bonuns count if array contains duplicates
    const check_duplicates = array.Versions.filter(
      (item) => item.IsLatest
    ).map(
      (item) => item.Key
    )
    console.log(`elements to check ${check_duplicates}`)
    const set = new Set(check_duplicates)
    const duplicates = check_duplicates.filter((item) => {
      if (set.has(item)) {
        set.delete(item)
      } else {
        return item
      }
    })
    return duplicates
  }
}

module.exports = { utilities }
