using System.ComponentModel.DataAnnotations;

namespace Aggregator.Admin.Models
{
    public class Tick
    {
        public int Id { get; set; }

        [Display(Name = "Source")]
        public string Source { get; set; } = string.Empty;

        [Display(Name = "Ticker")]
        public string Ticker { get; set; } = string.Empty;

        [Display(Name = "Price")]
        public decimal Price { get; set; }

        [Display(Name = "Volume")]
        public decimal Volume { get; set; }

        [Display(Name = "Timestamp")]
        public DateTime Timestamp { get; set; }

        [Display(Name = "Created At")]
        public DateTime CreatedAt { get; set; }
    }
}